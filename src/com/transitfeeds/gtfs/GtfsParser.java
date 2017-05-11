package com.transitfeeds.gtfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.input.BOMInputStream;
import org.mozilla.universalchardet.UniversalDetector;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.csvreader.CsvReader;

public class GtfsParser {

    private File       mGtfsFile;
    private Connection mConnection;
    private List<String> mExclude = new ArrayList<String>();
    
    private final static String COPY_SEPARATOR = "\t";

    public GtfsParser(File gtfsFile, Connection connection) throws FileNotFoundException, SQLException, Exception {
        if (!gtfsFile.exists()) {
            throw new FileNotFoundException("GTFS file not found");
        }

        if (!gtfsFile.isDirectory()) {
            throw new Exception("GTFS path must be a directory");
        }

        mGtfsFile = gtfsFile;

        mConnection = connection;
        mConnection.setAutoCommit(false);
    }

    private static String[] TABLES = {
            "agency", "agency_id TEXT, agency_name TEXT, agency_timezone TEXT, agency_url TEXT, agency_lang TEXT, agency_phone TEXT, agency_fare_url TEXT", "agency_id",
            "stops", "stop_index INTEGER, stop_id TEXT, stop_code TEXT, stop_name TEXT, stop_desc TEXT, zone_index INTEGER, zone_id TEXT, stop_lat REAL, stop_lon REAL, location_type INTEGER, parent_station TEXT, parent_station_index INTEGER, wheelchair_boarding INTEGER, stop_url TEXT, stop_timezone TEXT", "stop_index,stop_id,stop_code,zone_id,zone_index",
            "routes", "route_index INTEGER, route_id TEXT, agency_id TEXT, route_short_name TEXT, route_long_name TEXT, route_desc TEXT, route_type INTEGER, route_color TEXT, route_text_color TEXT, route_url TEXT", "route_index,route_id,agency_id",
            "trips", "trip_index INTEGER, trip_id TEXT, route_index INTEGER, route_id TEXT, service_index INTEGER, service_id TEXT, shape_index INTEGER, shape_id TEXT, trip_headsign TEXT, trip_short_name TEXT, direction_id INTEGER, block_index INTEGER, block_id TEXT, wheelchair_accessible INTEGER, departure_time TEXT, departure_time_secs INTEGER, arrival_time TEXT, arrival_time_secs INTEGER", "trip_index,route_index,service_index,shape_index,trip_id,route_id,block_index",
            "stop_times", "stop_index INTEGER, stop_id TEXT, trip_index INTEGER, trip_id TEXT, arrival_time TEXT, arrival_time_secs INTEGER, departure_time TEXT, departure_time_secs INTEGER, stop_sequence INTEGER, last_stop INTEGER, shape_dist_traveled REAL, stop_headsign TEXT, pickup_type INTEGER, drop_off_type INTEGER", "stop_index,trip_index,stop_id,trip_id",
            "calendar", "service_index INTEGER, service_id TEXT, monday INTEGER, tuesday INTEGER, wednesday INTEGER, thursday INTEGER, friday INTEGER, saturday INTEGER, sunday INTEGER, start_date TEXT, end_date TEXT", "service_index,service_id",
            "calendar_dates", "service_index INTEGER, service_id TEXT, date TEXT, exception_type INTEGER", "service_index",
            "shapes", "shape_index INTEGER, shape_id TEXT, shape_pt_lat REAL, shape_pt_lon REAL, shape_pt_sequence INTEGER, shape_dist_traveled REAL", "shape_index,shape_id", 
            "fare_attributes", "fare_index INTEGER, fare_id TEXT, price TEXT, currency_type TEXT, payment_method TEXT, transfers TEXT, transfer_duration TEXT", "fare_index,fare_id",
            "fare_rules", "fare_index INTEGER, route_index INTEGER, origin_index INTEGER, destination_index INTEGER, contains_index INTEGER", "fare_index", 
            "frequencies", "trip_index INTEGER, start_time TEXT, end_time TEXT, headway_secs TEXT, exact_times TEXT", "trip_index",
            "transfers", "from_stop_index INTEGER, to_stop_index INTEGER, transfer_type TEXT, min_transfer_time TEXT", "from_stop_index,to_stop_index",
            "feed_info", "feed_publisher_name TEXT, feed_publisher_url TEXT, feed_lang TEXT, feed_start_date TEXT, feed_end_date TEXT, feed_version TEXT", "",
            "perimetre_tr_plateforme_stif", "MonitoringRef_ZDE TEXT, reflex_lda_id TEXT, reflex_lda_nom TEXT ,reflex_zdl_id TEXT, reflex_zdl_nom TEXT, reflex_zde_id TEXT, reflex_zde_nom TEXT, gtfs_stop_id TEXT, Lineref TEXT, gtfs_line_name TEXT, codifligne_line_id TEXT, codifligne_line_externalcode TEXT, destination_code TEXT, codifligne_network_name TEXT, gtfs_agency TEXT, opendata_date TEXT, Dispo TEXT, reflex_zde_x TEXT, reflex_zde_y TEXT, xy TEXT", "" 
    };


    public void parse() throws Exception {
        createGtfsTables();
        parseFiles();
        createIndexes();
    }

    private void createGtfsTables() throws SQLException {
        ResultSet tables = mConnection.getMetaData().getTables(null, null, null, null);

        Set<String> tableNames = new HashSet<String>();

        while (tables.next()) {
            tableNames.add(tables.getString("TABLE_NAME"));
        }

        for (int i = 0; i < TABLES.length; i += 3) {
            String tableName = TABLES[i];

            if (tableNames.contains(tableName)) {
                String query = "DROP TABLE " + tableName;

                Statement stmt = mConnection.createStatement();
                System.err.println(query);
                stmt.execute(query);
                stmt.close();
            }

            String query = String.format("CREATE TABLE %s (%s)", tableName, TABLES[i + 1]);
            System.err.println(query);

            Statement stmt = mConnection.createStatement();
            stmt.execute(query);
            stmt.close();
        }

        mConnection.commit();
    }

    private void createIndexes() throws SQLException {
        for (int i = 0; i < TABLES.length; i += 3) {
            if (TABLES[i + 2].length() == 0) {
                continue;
            }
            
            String table = TABLES[i];
            String[] fields = TABLES[i + 2].split(",");

            for (int j = 0; j < fields.length; j++) {
                String query = String.format("CREATE INDEX %s_%s ON %s (%s)", table, fields[j], table, fields[j]);
                System.err.println(query);

                Statement stmt = mConnection.createStatement();
                stmt.execute(query);
                stmt.close();
            }
        }

        mConnection.commit();
    }
    

    private File getFile(String filename) {
        return new File(mGtfsFile.getAbsolutePath() + System.getProperty("file.separator") + filename);
    }

    private CsvReader getCsv(File f) throws FileNotFoundException, IOException {
        byte[] buf = new byte[4096];
        java.io.FileInputStream fis = new java.io.FileInputStream(f);

        UniversalDetector detector = new UniversalDetector(null);

        int nread;
        while ((nread = fis.read(buf)) > 0 && !detector.isDone()) {
            detector.handleData(buf, 0, nread);
        }

        detector.dataEnd();
        fis.close();

        String encoding = detector.getDetectedCharset();
        Charset charset;
        if (encoding != null) {
            charset = Charset.forName(encoding);
        }
        else {
            charset = Charset.forName("ISO-8859-1");
        }

        detector.reset();

        InputStream is = new BOMInputStream(new FileInputStream(f), false);
        return new CsvReader(is, ',', charset);
    }

    private void parseFiles() throws Exception {
        for (int i = 0; i < TABLES.length; i += 3) {
            String filename = TABLES[i] + ".txt";

            if (mExclude.contains(filename)) {
                continue;
            }

            File f = getFile(filename);
            
            if (!f.exists()) {
                f = getFile(TABLES[i] + ".csv");
            }

            try {
                parseFile(f, TABLES[i]);
            } catch (Exception e) {
                // System.err.println(e.toString());
            }
        }
    }

    private String getList(String[] strs) {
        String ret = "";

        for (int i = 0; i < strs.length; i++) {
            if (i > 0) {
                ret += ", ";
            }

            ret += strs[i];
        }

        return ret;
    }

    private String getPlaceholders(int len) {
        String ret = "";

        for (int i = 0; i < len; i++) {
            if (i > 0) {
                ret += ", ";
            }
            ret += "?";
        }

        return ret;
    }

    private RowProcessor getProcessor(String table) throws Exception {
        if (table.equals("stop_times")) {
            return new StopTimesRowProcessor();
        }
        else if (table.equals("agency")) {
            return new AgencyRowProcessor();
        }
        else if (table.equals("routes")) {
            return new RouteRowProcessor();
        }
        else if (table.equals("stops")) {
            return new StopRowProcessor();
        }
        else if (table.equals("trips")) {
            return new TripRowProcessor();
        }
        else if (table.equals("calendar")) {
            return new CalendarRowProcessor();
        }
        else if (table.equals("calendar_dates")) {
            return new CalendarDateRowProcessor();
        }
        else if (table.equals("shapes")) {
            return new ShapeRowProcessor();
        }
        else if (table.equals("fare_attributes")) {
            return new FareAttributesRowProcessor();
        }
        else if (table.equals("fare_rules")) {
            return new FareRulesRowProcessor();
        }
        else if (table.equals("frequencies")) {
            return new FrequenciesRowProcessor();
        }
        else if (table.equals("transfers")) {
            return new TransfersRowProcessor();
        }
        else if (table.equals("feed_info")) {
            return new FeedInfoRowProcessor();
        }
        else if (table.equals("perimetre_tr_plateforme_stif")) {
            return new PerimetreStifRowProcessor();
        }
        throw new Exception("No processor found for " + table);
    }

    private void parseFile(File f, String table) throws Exception {
        if (!f.exists()) {
            return;
        }
        
        System.err.println("Parsing " + f.getAbsolutePath());

        RowProcessor rp = getProcessor(table);

        CopyIn copier = null;

        if (mConnection instanceof BaseConnection) {
            CopyManager cm = new CopyManager((BaseConnection) mConnection);
            copier = cm.copyIn("COPY " + rp.getTableName() + " (" + getList(rp.getFields()) + ") FROM STDIN WITH DELIMITER '" + COPY_SEPARATOR + "' NULL AS ''");
        }

        try {
            CsvReader csv = getCsv(f);
            csv.readHeaders();

            PreparedStatement insert = null;
            
            if (copier == null) {
                insert = rp.getPreparedStatement(mConnection);
            }

            int row = 0;

            while (csv.readRecord()) {
                rp.process(csv, insert, copier);
                
                if (insert != null) {
                    insert.addBatch();
                }

                if ((row % 10000) == 0) {
                    if (insert != null) {
                        insert.executeBatch();
                    }
                    
                    System.err.println(String.format("%d", row));
                }

                row++;
            }

            if (insert != null) {
                insert.executeBatch();
            }
            else if (copier != null) {
                copier.endCopy();
            }

            mConnection.commit();
            
            if (insert != null) {
                insert.close();
            }
        } catch (SQLException se) {
            System.err.println("SQLException: " + se.getLocalizedMessage());
        } catch (IOException ioe) {
            System.err.println("IOException: " + ioe.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Exception: " + e.getLocalizedMessage());
        }
    }
    private static Map<String, Integer> mMappedRouteIds   = new HashMap<String, Integer>();
    private static Map<String, Integer> mMappedServiceIds = new HashMap<String, Integer>();
    private static Map<String, Integer> mMappedTripIds    = new HashMap<String, Integer>();
    private static Map<String, Integer> mMappedStopIds    = new HashMap<String, Integer>();
    private static Map<String, Integer> mMappedZoneIds    = new HashMap<String, Integer>();
    private static Map<String, Integer> mMappedShapeIds   = new HashMap<String, Integer>();
    private static Map<String, Integer> mMappedBlockIds   = new HashMap<String, Integer>();
    private static Map<String, Integer> mMappedFareIds    = new HashMap<String, Integer>();

    public static int getMappedRouteId(String routeId) {
        return getMappedId(mMappedRouteIds, routeId);
    }

    public static int getMappedServiceId(String serviceId) {
        return getMappedId(mMappedServiceIds, serviceId);
    }

    public static int getMappedTripId(String tripId) {
        return getMappedId(mMappedTripIds, tripId);
    }

    public static int getMappedStopId(String stopId) {
        return getMappedId(mMappedStopIds, stopId);
    }

    public static int getMappedZoneId(String zoneId) {
        return getMappedId(mMappedZoneIds, zoneId);
    }

    public static int getMappedFareId(String fareId) {
        return getMappedId(mMappedFareIds, fareId);
    }

    public static int getMappedShapeId(String shapeId) {
        return getMappedId(mMappedShapeIds, shapeId);
    }

    public static int getMappedBlockId(String blockId) {
        return getMappedId(mMappedBlockIds, blockId);
    }

    private static int getMappedId(Map<String, Integer> map, String key) {
        if (key == null || key.length() == 0) {
            return 0;
        }
        
        Integer ret = map.get(key);

        if (ret == null) {
            ret = Integer.valueOf(map.size() + 1);
            map.put(key, ret);
        }

        return ret.intValue();
    }

    private abstract class RowProcessor {
        
        public abstract void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException;
        
        public abstract String getTableName();
        
        public abstract String[] getFields();

        final public PreparedStatement getPreparedStatement(Connection connection) throws SQLException {
            String[] fields = getFields();
            
            String query = String.format("INSERT INTO %s (%s) VALUES (%s)", getTableName(), getList(fields), getPlaceholders(fields.length));
            return connection.prepareStatement(query);
        }
    }

    private class AgencyRowProcessor extends RowProcessor {

        @Override
        public String[] getFields() {
            String fields[] = { "agency_id", "agency_name", "agency_timezone", "agency_url", "agency_lang", "agency_phone", "agency_fare_url" };
            return fields;
        }
        
        @Override
        public String getTableName() {
            return "agency";
        }
        
        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {
            int i = 0;

            String agencyId = csv.get("agency_id");

            if (copier == null) {
                insert.setString(++i, agencyId);
                insert.setString(++i, csv.get("agency_name"));
                insert.setString(++i, csv.get("agency_timezone"));
                insert.setString(++i, csv.get("agency_url"));
                insert.setString(++i, csv.get("agency_lang"));
                insert.setString(++i, csv.get("agency_phone"));
                insert.setString(++i, csv.get("agency_fare_url"));
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(agencyId);
                row.add(csv.get("agency_name"));
                row.add(csv.get("agency_timezone"));
                row.add(csv.get("agency_url"));
                row.add(csv.get("agency_lang"));
                row.add(csv.get("agency_phone"));
                row.add(csv.get("agency_fare_url"));
                row.write(copier, COPY_SEPARATOR);
            }
        }
    }

    private class RouteRowProcessor extends RowProcessor {
        @Override
        public String[] getFields() {
            String fields[] = { "route_index", "route_id", "agency_id", "route_short_name", "route_long_name", "route_desc", "route_type", "route_color", "route_text_color", "route_url" };
            return fields;
        }
        
        @Override
        public String getTableName() {
            return "routes";
        }

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {
            int i = 0;

            String routeId = csv.get("route_id");

            if (copier == null) {
                insert.setInt(++i, getMappedRouteId(routeId));
                insert.setString(++i, routeId);
                insert.setString(++i, csv.get("agency_id"));
                insert.setString(++i, csv.get("route_short_name"));
                insert.setString(++i, csv.get("route_long_name"));
                insert.setString(++i, csv.get("route_desc"));
                insert.setInt(++i, Integer.valueOf(csv.get("route_type")));
                insert.setString(++i, csv.get("route_color"));
                insert.setString(++i, csv.get("route_text_color"));
                insert.setString(++i, csv.get("route_url"));
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(getMappedRouteId(routeId));
                row.add(routeId);
                row.add(csv.get("agency_id"));
                row.add(csv.get("route_short_name"));
                row.add(csv.get("route_long_name"));
                row.add(csv.get("route_desc"));
                row.add(Integer.valueOf(csv.get("route_type")));
                row.add(csv.get("route_color"));
                row.add(csv.get("route_text_color"));
                row.add(csv.get("route_url"));
                
                row.write(copier, COPY_SEPARATOR);
            }
        }

    }

    private class StopRowProcessor extends RowProcessor {
        @Override
        public String[] getFields() {
            String fields[] = { "stop_index", "stop_id", "stop_code", "stop_name", "stop_desc", "zone_id", "zone_index", "stop_lat", "stop_lon", "location_type", "parent_station", "parent_station_index",
                    "wheelchair_boarding", "stop_url", "stop_timezone" };
            return fields;
        }
        
        @Override
        public String getTableName() {
            return "stops";
        }

        private int stopIdIdx;
        private int stopCodeIdx;
        private int stopNameIdx;
        private int stopDescIdx;
        private int zoneIdIdx;
        private int stopLatIdx;
        private int stopLonIdx;
        private int locationTypeIdx;
        private int parentStationIdx;
        private int wheelchairIdx;
        private int stopUrlIdx;
        private int stopTimezoneIdx;

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {
            if (csv.getCurrentRecord() == 0) {
                stopIdIdx = csv.getIndex("stop_id");
                stopCodeIdx = csv.getIndex("stop_code");
                stopNameIdx = csv.getIndex("stop_name");
                stopDescIdx = csv.getIndex("stop_desc");
                zoneIdIdx = csv.getIndex("zone_id");
                stopLatIdx = csv.getIndex("stop_lat");
                stopLonIdx = csv.getIndex("stop_lon");
                locationTypeIdx = csv.getIndex("location_type");
                parentStationIdx = csv.getIndex("parent_station");
                wheelchairIdx = csv.getIndex("wheelchair_boarding");
                stopUrlIdx = csv.getIndex("stop_url");
                stopTimezoneIdx = csv.getIndex("stop_timezone");
            }

            int i = 0;

            String stopId = csv.get(stopIdIdx);
            String parentId = csv.get(parentStationIdx);
            String zoneId = csv.get(zoneIdIdx);
            String stopUrl = csv.get(stopUrlIdx);
            String stopTimezone = csv.get(stopTimezoneIdx);

            int locationType = 0;

            try {
                locationType = Integer.valueOf(csv.get(locationTypeIdx));
            } catch (Exception e) {

            }

            int wheelchair = 0;

            try {
                wheelchair = Integer.valueOf(csv.get(wheelchairIdx));
            } catch (Exception e) {

            }

            if (copier == null) {
                insert.setInt(++i, getMappedStopId(stopId));
                insert.setString(++i, stopId);
                insert.setString(++i, csv.get(stopCodeIdx));
                insert.setString(++i, csv.get(stopNameIdx));
                insert.setString(++i, csv.get(stopDescIdx));
                insert.setString(++i, zoneId);
                insert.setInt(++i, getMappedZoneId(zoneId));
    
                insert.setDouble(++i, Double.valueOf(csv.get(stopLatIdx)));
                insert.setDouble(++i, Double.valueOf(csv.get(stopLonIdx)));
    
                insert.setInt(++i, locationType);
    
                if (parentId != null && parentId.length() > 0) {
                    insert.setString(++i, parentId);
                    insert.setInt(++i, getMappedStopId(parentId));
                }
                else {
                    insert.setNull(++i, Types.VARCHAR);
                    insert.setNull(++i, Types.INTEGER);
                }
    
                insert.setInt(++i, wheelchair);
                insert.setString(++i, stopUrl);
                insert.setString(++i, stopTimezone);
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(getMappedStopId(stopId));
                row.add(stopId);
                row.add(csv.get(stopCodeIdx));
                row.add(csv.get(stopNameIdx));
                row.add(csv.get(stopDescIdx));
                row.add(zoneId);
                row.add(getMappedZoneId(zoneId));
                row.add(Double.valueOf(csv.get(stopLatIdx)));
                row.add(Double.valueOf(csv.get(stopLonIdx)));
                row.add(locationType);
                
                if (parentId != null && parentId.length() > 0) {
                    row.add(parentId);
                    row.add(getMappedStopId(parentId));
                }
                else {
                    row.addNull(2);
                }
    
                row.add(wheelchair);
                row.add(stopUrl);
                row.add(stopTimezone);
                
                row.write(copier, COPY_SEPARATOR);
            }
        }

    }

    private class TripRowProcessor extends RowProcessor {
        
        @Override
        public String[] getFields() {
            String fields[] = { "trip_index", "trip_id", "route_index", "route_id", "service_index", "shape_index", "block_index", "block_id", "trip_headsign", "trip_short_name", "direction_id",
            "wheelchair_accessible", "shape_id", "service_id" };
            return fields;
        }
        
        @Override
        public String getTableName() {
            return "trips";
        }

        private int tripIdIdx;
        private int routeIdIdx;
        private int serviceIdIdx;
        private int shapeIdIdx;
        private int blockIdIdx;
        private int tripHeadsignIdx;
        private int tripShortNameIdx;
        private int directionIdIdx;
        private int wheelchairIdx;

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {

            if (csv.getCurrentRecord() == 0) {
                tripIdIdx = csv.getIndex("trip_id");
                routeIdIdx = csv.getIndex("route_id");
                serviceIdIdx = csv.getIndex("service_id");
                blockIdIdx = csv.getIndex("block_id");
                shapeIdIdx = csv.getIndex("shape_id");
                tripHeadsignIdx = csv.getIndex("trip_headsign");
                tripShortNameIdx = csv.getIndex("trip_short_name");
                directionIdIdx = csv.getIndex("direction_id");
                wheelchairIdx = csv.getIndex("wheelchair_accessible");
            }

            int i = 0;

            String tripId = csv.get(tripIdIdx);
            String routeId = csv.get(routeIdIdx);
            String serviceId = csv.get(serviceIdIdx);
            String shapeId = csv.get(shapeIdIdx);
            String blockId = csv.get(blockIdIdx);

            int directionId = -1;

            try {
                directionId = Integer.valueOf(csv.get(directionIdIdx));
            } catch (Exception e) {
            }

            int wheelchair = 0;

            try {
                wheelchair = Integer.valueOf(csv.get(wheelchairIdx));
            } catch (Exception e) {
            }

            if (copier == null) {
                insert.setInt(++i, getMappedTripId(tripId));
                insert.setString(++i, tripId);
                insert.setInt(++i, getMappedRouteId(routeId));
                insert.setString(++i, routeId);
                insert.setInt(++i, getMappedServiceId(serviceId));
                insert.setInt(++i, getMappedShapeId(shapeId));
                insert.setInt(++i, getMappedBlockId(blockId));
                insert.setString(++i, blockId);
                insert.setString(++i, csv.get(tripHeadsignIdx));
                insert.setString(++i, csv.get(tripShortNameIdx));
                
                if (directionId < 0) {
                    insert.setNull(++i, java.sql.Types.INTEGER);
                }
                else {
                    insert.setInt(++i, directionId);
                }
                
                insert.setInt(++i, wheelchair);
                insert.setString(++i, shapeId);
                insert.setString(++i, serviceId);
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(getMappedTripId(tripId));
                row.add(tripId);
                row.add(getMappedRouteId(routeId));
                row.add(routeId);
                row.add(getMappedServiceId(serviceId));
                row.add(getMappedShapeId(shapeId));
                row.add(getMappedBlockId(blockId));
                row.add(blockId);
                row.add(csv.get(tripHeadsignIdx));
                row.add(csv.get(tripShortNameIdx));
                
                if (directionId < 0) {
                    row.addNull();
                }
                else {
                    row.add(directionId);
                }
                
                row.add(wheelchair);
                row.add(shapeId);
                row.add(serviceId);
                
                row.write(copier, COPY_SEPARATOR);
            }
        }

    }

    private class CalendarRowProcessor extends RowProcessor {
        @Override
        public String[] getFields() {
            String fields[] = { "service_index", "service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date" };
            return fields;
        }
        
        @Override
        public String getTableName() {
            return "calendar";
        }

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {
            int i = 0;

            String serviceId = csv.get("service_id");
            
            if (copier == null) {
                insert.setInt(++i, getMappedServiceId(serviceId));
                insert.setString(++i, serviceId);
    
                insert.setInt(++i, csv.get("monday").equals("1") ? 1 : 0);
                insert.setInt(++i, csv.get("tuesday").equals("1") ? 1 : 0);
                insert.setInt(++i, csv.get("wednesday").equals("1") ? 1 : 0);
                insert.setInt(++i, csv.get("thursday").equals("1") ? 1 : 0);
                insert.setInt(++i, csv.get("friday").equals("1") ? 1 : 0);
                insert.setInt(++i, csv.get("saturday").equals("1") ? 1 : 0);
                insert.setInt(++i, csv.get("sunday").equals("1") ? 1 : 0);
    
                insert.setString(++i, csv.get("start_date"));
                insert.setString(++i, csv.get("end_date"));
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(getMappedServiceId(serviceId));
                row.add(serviceId);
    
                row.add(csv.get("monday").equals("1") ? 1 : 0);
                row.add(csv.get("tuesday").equals("1") ? 1 : 0);
                row.add(csv.get("wednesday").equals("1") ? 1 : 0);
                row.add(csv.get("thursday").equals("1") ? 1 : 0);
                row.add(csv.get("friday").equals("1") ? 1 : 0);
                row.add(csv.get("saturday").equals("1") ? 1 : 0);
                row.add(csv.get("sunday").equals("1") ? 1 : 0);
    
                row.add(csv.get("start_date"));
                row.add(csv.get("end_date"));
                
                row.write(copier, COPY_SEPARATOR);
            }
        }

    }

    private class CalendarDateRowProcessor extends RowProcessor {
        
        @Override
        public String[] getFields() {
            String fields[] = { "service_index", "service_id", "date", "exception_type" };
            return fields;
        }

        @Override
        public String getTableName() {
            return "calendar_dates";
        }

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {
            int i = 0;

            String serviceId = csv.get("service_id");
            
            if (copier == null) {
                insert.setInt(++i, getMappedServiceId(serviceId));
                insert.setString(++i, serviceId);
                insert.setString(++i, csv.get("date"));
                insert.setInt(++i, Integer.valueOf(csv.get("exception_type")));
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(getMappedServiceId(serviceId));
                row.add(serviceId);
                row.add(csv.get("date"));
                row.add(Integer.valueOf(csv.get("exception_type")));                
                row.write(copier, COPY_SEPARATOR);
            }
        }

    }

    private class StopTimesRowProcessor extends RowProcessor {
        
        @Override
        public String[] getFields() {
            String fields[] = { "trip_index", "trip_id", "stop_index", "stop_id" , "arrival_time", "arrival_time_secs", "departure_time", "departure_time_secs", "stop_sequence", "last_stop", "shape_dist_traveled", "stop_headsign", "pickup_type", "drop_off_type" };
            return fields;
        }
        
        @Override
        public String getTableName() {
            return "stop_times";
        }

        private int tripIdIdx;
        private int arrivalTimeIdx;
        private int departureTimeIdx;
        private int stopIdIdx;
        private int stopSequenceIdx;
        private int shapeDistTraveledIdx;
        private int stopHeadsignIdx;
        private int pickupTypeIdx;
        private int dropOffTypeIdx;

        private int getSeconds(String hms, long row) {
            String parts[] = hms.split("\\:", 3);
            
            if (parts.length != 3) {
                return -1;
            }
            
            return Integer.valueOf(parts[0]) * 3600 + Integer.valueOf(parts[1]) * 60 + Integer.valueOf(parts[2]);
        }

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {

            long rowNumber = csv.getCurrentRecord();
            
            if (rowNumber == 0) {
                tripIdIdx = csv.getIndex("trip_id");
                arrivalTimeIdx = csv.getIndex("arrival_time");
                departureTimeIdx = csv.getIndex("departure_time");
                stopIdIdx = csv.getIndex("stop_id");
                stopSequenceIdx = csv.getIndex("stop_sequence");
                shapeDistTraveledIdx = csv.getIndex("shape_dist_traveled");
                stopHeadsignIdx = csv.getIndex("stop_headsign");
                pickupTypeIdx = csv.getIndex("pickup_type");
                dropOffTypeIdx = csv.getIndex("drop_off_type");
            }

            int i = 0;

            String tripId = csv.get(tripIdIdx);
            String stopId = csv.get(stopIdIdx);
            String arrivalTime = csv.get(arrivalTimeIdx);
            String departureTime = csv.get(departureTimeIdx);
            
            String shapeDistTraveled = csv.get(shapeDistTraveledIdx);
            String stopHeadsign = csv.get(stopHeadsignIdx);
            
            if (stopHeadsign != null) {
                stopHeadsign = stopHeadsign.trim();
                
                if (stopHeadsign.length() == 0) {
                    stopHeadsign = null;
                }
            }
            
            int pickupType = -1;
            int dropOffType = -1;
            
            try {
                pickupType = Integer.valueOf(csv.get(pickupTypeIdx));
            }
            catch (Exception e) {
            }
            
            try {
                dropOffType = Integer.valueOf(csv.get(dropOffTypeIdx));
            }
            catch (Exception e) {
            }
            
            if (copier == null) {
                insert.setInt(++i, getMappedTripId(tripId));
                insert.setString(++i, tripId);
                insert.setInt(++i, getMappedStopId(stopId));
                insert.setString(++i, stopId);
    
                insert.setString(++i, arrivalTime);
                
                int secs = getSeconds(arrivalTime, rowNumber);
                
                if (secs < 0) {
                    insert.setNull(++i, java.sql.Types.INTEGER);
                }
                else {                
                    insert.setInt(++i, secs);
                }
    
                insert.setString(++i, departureTime);
                
                secs = getSeconds(departureTime, rowNumber);
                if (secs < 0) {
                    insert.setNull(++i, java.sql.Types.INTEGER);
                }
                else {                
                    insert.setInt(++i, secs);
                }
    
                insert.setInt(++i, Integer.valueOf(csv.get(stopSequenceIdx)));
    
                insert.setInt(++i, 0);
                
                if (shapeDistTraveled != null && shapeDistTraveled.length() > 0) {
                    insert.setDouble(++i, Double.valueOf(shapeDistTraveled));
                }
                else {
                    insert.setNull(++i, java.sql.Types.DOUBLE);
                }
                
                if (stopHeadsign == null) {
                    insert.setNull(++i, java.sql.Types.VARCHAR);
                }
                else {
                    insert.setString(++i, stopHeadsign);
                }
                
                if (pickupType < 0) {
                    insert.setNull(++i, java.sql.Types.INTEGER);
                }
                else {
                    insert.setInt(++i, pickupType);
                }
                
                if (dropOffType < 0) {
                    insert.setNull(++i, java.sql.Types.INTEGER);
                }
                else {
                    insert.setInt(++i, dropOffType);
                }
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(getMappedTripId(tripId));
                row.add(tripId);
                row.add(getMappedStopId(stopId));
                row.add(stopId);
                row.add(arrivalTime);
                row.add(getSeconds(arrivalTime, rowNumber));
                row.add(departureTime);
                row.add(getSeconds(departureTime, rowNumber));
                row.add(Integer.valueOf(csv.get(stopSequenceIdx)));
                row.add(0);
                
                if (shapeDistTraveled != null && shapeDistTraveled.length() > 0) {
                    row.add(Double.valueOf(shapeDistTraveled));
                }
                else {
                    row.addNull();
                }
                
                if (stopHeadsign == null) {
                    row.addNull();
                }
                else {
                    row.add(stopHeadsign);
                }
                
                if (pickupType < 0) {
                    row.addNull();
                }
                else {
                    row.add(pickupType);
                }
                
                if (dropOffType < 0) {
                    row.addNull();
                }
                else {
                    row.add(dropOffType);
                }
                
                
                row.write(copier, COPY_SEPARATOR);
            }
        }
    }

    private class ShapeRowProcessor extends RowProcessor {
        @Override
        public String[] getFields() {
            String fields[] = { "shape_index", "shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled" };
            return fields;
        }
        
        @Override
        public String getTableName() {
            return "shapes";
        }

        private int shapeIdIdx;
        private int latIdx;
        private int lonIdx;
        private int sequenceIdx;
        private int shapeDistTraveledIdx;

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {

            if (csv.getCurrentRecord() == 0) {
                shapeIdIdx = csv.getIndex("shape_id");
                latIdx = csv.getIndex("shape_pt_lat");
                lonIdx = csv.getIndex("shape_pt_lon");
                sequenceIdx = csv.getIndex("shape_pt_sequence");
                shapeDistTraveledIdx = csv.getIndex("shape_dist_traveled");
            }

            int i = 0;

            String shapeId = csv.get(shapeIdIdx);
            String shapeDistTraveled = csv.get(shapeDistTraveledIdx);
            
            if (copier == null) {
                insert.setInt(++i, getMappedShapeId(shapeId));
                insert.setString(++i, shapeId);
    
                insert.setDouble(++i, Double.valueOf(csv.get(latIdx)));
                insert.setDouble(++i, Double.valueOf(csv.get(lonIdx)));
    
                insert.setInt(++i, Integer.valueOf(csv.get(sequenceIdx)));
                
                if (shapeDistTraveled != null && shapeDistTraveled.length() > 0) {
                    insert.setDouble(++i, Double.valueOf(shapeDistTraveled));
                }
                else {
                    insert.setNull(++i, java.sql.Types.DOUBLE);
                }
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(getMappedShapeId(shapeId));
                row.add(shapeId);
    
                row.add(Double.valueOf(csv.get(latIdx)));
                row.add(Double.valueOf(csv.get(lonIdx)));
    
                row.add(Integer.valueOf(csv.get(sequenceIdx)));
                
                if (shapeDistTraveled != null && shapeDistTraveled.length() > 0) {
                    row.add(Double.valueOf(shapeDistTraveled));
                }
                else {
                    row.addNull();
                }

                row.write(copier, COPY_SEPARATOR);
            }
        }
    }
    
    private class FareAttributesRowProcessor extends RowProcessor {

        @Override
        public String[] getFields() {
            String fields[] = { "fare_index", "fare_id", "price", "currency_type", "payment_method", "transfers", "transfer_duration" };
            return fields;
        }

        @Override
        public String getTableName() {
            return "fare_attributes";
        }

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {
            int i = 0;
            
            String fareId = csv.get("fare_id");
            
            if (copier == null) {
                insert.setInt(++i, getMappedFareId(fareId));
                insert.setString(++i, fareId);
                insert.setString(++i, csv.get("price"));
                insert.setString(++i, csv.get("currency_type"));
                insert.setString(++i, csv.get("payment_method"));
                insert.setString(++i, csv.get("transfers"));
                insert.setString(++i, csv.get("transfer_duration"));
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(getMappedFareId(fareId));
                row.add(fareId);
                row.add(csv.get("price"));
                row.add(csv.get("currency_type"));
                row.add(csv.get("payment_method"));
                row.add(csv.get("transfers"));
                row.add(csv.get("transfer_duration"));
                
                row.write(copier, COPY_SEPARATOR);
            }            
        }
    }
    
    private class FareRulesRowProcessor extends RowProcessor {
        @Override
        public String[] getFields() {
            String fields[] = { "fare_index", "route_index", "origin_index", "destination_index", "contains_index" };
            return fields;
        }

        @Override
        public String getTableName() {
            return "fare_rules";
        }

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {
            
            if (copier == null) {
                int i = 0;
                insert.setInt(++i, getMappedFareId(csv.get("fare_id")));
                insert.setInt(++i, getMappedRouteId(csv.get("route_id")));
                insert.setInt(++i, getMappedZoneId(csv.get("origin_id")));
                insert.setInt(++i, getMappedZoneId(csv.get("destination_id")));
                insert.setInt(++i, getMappedZoneId(csv.get("contains_id")));
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(getMappedFareId(csv.get("fare_id")));
                row.add(getMappedRouteId(csv.get("route_id")));
                row.add(getMappedZoneId(csv.get("origin_id")));
                row.add(getMappedZoneId(csv.get("destination_id")));
                row.add(getMappedZoneId(csv.get("contains_id")));
                
                row.write(copier, COPY_SEPARATOR);
            }
        }
    }
    
    private class FrequenciesRowProcessor extends RowProcessor {
        @Override
        public String[] getFields() {
            String fields[] = { "trip_index", "start_time", "end_time", "headway_secs", "exact_times" };
            return fields;
        }

        @Override
        public String getTableName() {
            return "frequencies";
        }

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {
            if (copier == null) {
                int i = 0;
                insert.setInt(++i, getMappedTripId(csv.get("trip_id")));
                insert.setString(++i, csv.get("start_time"));
                insert.setString(++i, csv.get("end_time"));
                insert.setString(++i, csv.get("headway_secs"));
                insert.setString(++i, csv.get("exact_times"));
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(getMappedTripId(csv.get("trip_id")));
                row.add(csv.get("start_time"));
                row.add(csv.get("end_time"));
                row.add(csv.get("headway_secs"));
                row.add(csv.get("exact_times"));
                
                row.write(copier, COPY_SEPARATOR);
            }
            
        }
    }
    
    private class TransfersRowProcessor extends RowProcessor {
//        "transfers", "from_stop_index INTEGER, to_stop_index INTEGER, transfer_type TEXT, min_transfer_time TEXT", "from_stop_index,to_stop_index",
        
        @Override
        public String[] getFields() {
            String fields[] = { "from_stop_index", "to_stop_index", "transfer_type", "min_transfer_time" };
            return fields;
        }

        @Override
        public String getTableName() {
            return "transfers";
        }

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {
            if (copier == null) {
                int i = 0;
                insert.setInt(++i, getMappedStopId(csv.get("from_stop_id")));
                insert.setInt(++i, getMappedStopId(csv.get("to_stop_id")));
                insert.setString(++i, csv.get("transfer_type"));
                insert.setString(++i, csv.get("min_transfer_time"));
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(getMappedStopId(csv.get("from_stop_id")));
                row.add(getMappedStopId(csv.get("to_stop_id")));
                row.add(csv.get("transfer_type"));
                row.add(csv.get("min_transfer_time"));
                
                row.write(copier, COPY_SEPARATOR);
            }
        }
    }
    
    private class FeedInfoRowProcessor extends RowProcessor {
//        "feed_info", "feed_publisher_name TEXT, feed_publisher_url TEXT, feed_lang TEXT, feed_start_date TEXT, feed_end_date TEXT, feed_version TEXT", "" 
        
        @Override
        public String[] getFields() {
            String fields[] = { "feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date", "feed_end_date", "feed_version" };
            return fields;
        }

        @Override
        public String getTableName() {
            return "feed_info";
        }

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {
            if (copier == null) {
                int i = 0;
                insert.setString(++i, csv.get("feed_publisher_name"));
                insert.setString(++i, csv.get("feed_publisher_url"));
                insert.setString(++i, csv.get("feed_lang"));
                insert.setString(++i, csv.get("feed_start_date"));
                insert.setString(++i, csv.get("feed_end_date"));
                insert.setString(++i, csv.get("feed_version"));
            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(csv.get("feed_publisher_name"));
                row.add(csv.get("feed_publisher_url"));
                row.add(csv.get("feed_lang"));
                row.add(csv.get("feed_start_date"));
                row.add(csv.get("feed_end_date"));
                row.add(csv.get("feed_version"));
                
                row.write(copier, COPY_SEPARATOR);
            }
        }
    }

    private class PerimetreStifRowProcessor extends RowProcessor {
//        "feed_info", "feed_publisher_name TEXT, feed_publisher_url TEXT, feed_lang TEXT, feed_start_date TEXT, feed_end_date TEXT, feed_version TEXT", "" 
        
        @Override
        public String[] getFields() {
            String fields[] = { "MonitoringRef_ZDE", "reflex_lda_id", "reflex_lda_nom", "reflex_zdl_id", "reflex_zdl_nom", "reflex_zde_id", "reflex_zde_nom", "gtfs_stop_id", "Lineref", "gtfs_line_name", "codifligne_line_id", "codifligne_line_externalcode", "destination_code", "codifligne_network_name", "gtfs_agency", "opendata_date", "Dispo", "reflex_zde_x", "reflex_zde_y", "xy" };
            return fields;
        }

        @Override
        public String getTableName() {
            return "perimetre_tr_plateforme_stif";
        }

        @Override
        public void process(CsvReader csv, PreparedStatement insert, CopyIn copier) throws SQLException, IOException {
            if (copier == null) {
                int i = 0;
                insert.setString(++i, csv.get("MonitoringRef_ZDE"));
                insert.setString(++i, csv.get("reflex_lda_id"));
                insert.setString(++i, csv.get("reflex_lda_nom"));
                insert.setString(++i, csv.get("reflex_zdl_id"));
                insert.setString(++i, csv.get("reflex_zdl_nom"));
                insert.setString(++i, csv.get("reflex_zde_id"));
                insert.setString(++i, csv.get("reflex_zde_nom"));
                insert.setString(++i, csv.get("gtfs_stop_id"));
                insert.setString(++i, csv.get("Lineref"));
                insert.setString(++i, csv.get("gtfs_line_name"));
                insert.setString(++i, csv.get("codifligne_line_id"));
                insert.setString(++i, csv.get("codifligne_line_externalcode"));
                insert.setString(++i, csv.get("destination_code"));
                insert.setString(++i, csv.get("codifligne_network_name"));
                insert.setString(++i, csv.get("gtfs_agency"));
                insert.setString(++i, csv.get("opendata_date"));
                insert.setString(++i, csv.get("Dispo"));
                insert.setString(++i, csv.get("reflex_zde_x"));
                insert.setString(++i, csv.get("reflex_zde_y"));
                insert.setString(++i, csv.get("xy"));

            }
            else {
                DataCopierRow row = new DataCopierRow();
                row.add(csv.get("MonitoringRef_ZDE"));
                row.add(csv.get("reflex_lda_id"));
                row.add(csv.get("reflex_lda_nom"));
                row.add(csv.get("reflex_zdl_id"));
                row.add(csv.get("reflex_zdl_nom"));
                row.add(csv.get("reflex_zde_id"));
                row.add(csv.get("reflex_zde_nom"));
                row.add(csv.get("gtfs_stop_id"));
                row.add(csv.get("Lineref"));
                row.add(csv.get("gtfs_line_name"));
                row.add(csv.get("codifligne_line_id"));
                row.add(csv.get("codifligne_line_externalcode"));
                row.add(csv.get("destination_code"));
                row.add(csv.get("codifligne_network_name"));
                row.add(csv.get("gtfs_agency"));
                row.add(csv.get("opendata_date"));
                row.add(csv.get("Dispo"));
                row.add(csv.get("reflex_zde_x"));
                row.add(csv.get("reflex_zde_y"));
                row.add(csv.get("xy"));
                
                row.write(copier, COPY_SEPARATOR);
            }
        }
    }  
    
    public void exclude(String filename) {
        mExclude.add(filename);
    }
}
