package com.transitfeeds.gtfs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class GtfsOptimizer {
    private Connection mConnection;

    public GtfsOptimizer(Connection connection) {
        mConnection = connection;
    }

    public void optimize() throws SQLException {
        updateStopSequence();
        calculateTripTimes();
        finalize();
    }

    public void finalize() throws SQLException {
        mConnection.setAutoCommit(true);
        
        String[] queries = {
//                "DELETE FROM stops WHERE stop_index NOT IN (SELECT DISTINCT stop_index FROM stop_times)",
                "VACUUM",
                "ANALYZE"
        };
        
        Statement st = mConnection.createStatement();

        for (int i = 0; i < queries.length; i++) {
            String query = queries[i];

            System.err.println(query);
            st.executeUpdate(query);
        }
        
        st.close();
        
        System.err.println("DONE");
    }
    
    private void updateStopSequence() throws SQLException {
        Statement st = mConnection.createStatement();
        ResultSet result = st.executeQuery("SELECT trip_index FROM trips");

        PreparedStatement update = mConnection.prepareStatement("UPDATE stop_times SET last_stop = 1 WHERE trip_index = ? AND stop_sequence = (SELECT max(stop_sequence) FROM stop_times WHERE trip_index = ?)");

        int row = 0;
        
        while (result.next()) {
            int tripIndex = result.getInt(1);

            update.setInt(1, tripIndex);
            update.setInt(2, tripIndex);
            
            update.addBatch();
            
            if ((row % 1000) == 0) {
                update.executeBatch();
                System.err.println(String.format("%d", row));
            }
            
            row++;
        }
        
        update.executeBatch();
        mConnection.commit();
        
        st.close();
        update.close();
    }

    private void calculateTripTimes() throws SQLException {
        Statement st = mConnection.createStatement();
        ResultSet result = st.executeQuery("SELECT trip_index FROM trips");
        
        PreparedStatement update = mConnection.prepareStatement("UPDATE trips SET departure_time = ?, departure_time_secs = ?, arrival_time = ?, arrival_time_secs = ? WHERE trip_index = ?");

        PreparedStatement select = mConnection.prepareStatement("SELECT arrival_time, arrival_time_secs, departure_time, departure_time_secs FROM stop_times WHERE trip_index = ? ORDER BY stop_sequence");
        
        int row = 0;
        
        while (result.next()) {
            int tripIndex = result.getInt(1);

            select.setInt(1, tripIndex);
            ResultSet stopTimes = select.executeQuery();

            int arrivalTimeSecs = -1;
            int departureTimeSecs = -1;
            String arrivalTime = null;
            String departureTime = null;
            
            int i = 0;
            
            while (stopTimes.next()) {
                if (i++ == 0) {
                    departureTime = stopTimes.getString(3);
                    departureTimeSecs = stopTimes.getInt(4);
                }
                
                arrivalTime = stopTimes.getString(1);
                arrivalTimeSecs = stopTimes.getInt(2);
            }
            
            stopTimes.close();
            
            update.setString(1, departureTime);
            update.setInt(2, departureTimeSecs);
            update.setString(3, arrivalTime);
            update.setInt(4, arrivalTimeSecs);
            update.setInt(5, tripIndex);
            
            update.addBatch();
            
            if ((row % 1000) == 0) {
                update.executeBatch();
                System.err.println(String.format("%d", row));
            }
            
            row++;
        }
        
        update.executeBatch();
        mConnection.commit();
        
        select.close();
        st.close();
        update.close();
        result.close();
    }

}
