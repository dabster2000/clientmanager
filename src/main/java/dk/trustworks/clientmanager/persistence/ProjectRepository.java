package dk.trustworks.clientmanager.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.framework.persistence.GenericRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by hans on 17/03/15.
 */
public class ProjectRepository extends GenericRepository {

    private static final Logger logger = LogManager.getLogger();

    public ProjectRepository() {
        super();
    }

    public List<Map<String, Object>> findByOrderByNameAsc() {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM project ORDER BY name ASC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ResultSet resultSet = stmt.executeQuery();
            result = getEntitiesFromResultSet(resultSet);
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public List<Map<String, Object>> findByActiveTrueOrderByNameAsc() {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM project WHERE active = TRUE ORDER BY name ASC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ResultSet resultSet = stmt.executeQuery();
            result = getEntitiesFromResultSet(resultSet);
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public List<Map<String, Object>> findByClientUUID(String clientUUID) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM project WHERE clientuuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, clientUUID);
            ResultSet resultSet = stmt.executeQuery();
            result = getEntitiesFromResultSet(resultSet);
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public List<Map<String, Object>> findByClientUUIDAndActiveTrue(String clientUUID) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM project WHERE clientuuid LIKE ? AND active = TRUE", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, clientUUID);
            ResultSet resultSet = stmt.executeQuery();
            result = getEntitiesFromResultSet(resultSet);
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public List<Map<String, Object>> findByClientUUIDOrderByNameAsc(String clientUUID) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM project WHERE clientuuid LIKE ? ORDER BY name ASC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, clientUUID);
            ResultSet resultSet = stmt.executeQuery();
            result = getEntitiesFromResultSet(resultSet);
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public List<Map<String, Object>> findByClientUUIDAndActiveTrueOrderByNameAsc(String clientUUID) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM project WHERE clientuuid LIKE ? AND active = TRUE ORDER BY name ASC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, clientUUID);
            ResultSet resultSet = stmt.executeQuery();
            result = getEntitiesFromResultSet(resultSet);
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public void create(JsonNode jsonNode) {
        logger.debug("Create project: " + jsonNode);
        testForNull(jsonNode, new String[]{"clientuuid", "clientdatauuid"});
        logger.debug("ProjectRepository.create");
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("INSERT INTO project (uuid, active, budget, clientuuid, created, customerreference, name, userowneruuid, clientdatauuid, startdate, enddate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, (jsonNode.get("uuid").asText() != "") ? jsonNode.get("uuid").asText() : UUID.randomUUID().toString());
            stmt.setBoolean(2, (jsonNode.get("active").asBoolean(true)));
            stmt.setDouble(3, (jsonNode.get("budget").asText() != "") ? jsonNode.get("budget").asDouble() : 0.0);
            stmt.setString(4, jsonNode.get("clientuuid").asText());
            stmt.setDate(5, new Date(new java.util.Date().getTime()));
            stmt.setString(6, (jsonNode.get("customerreference").asText() != "") ? jsonNode.get("customerreference").asText() : "");
            stmt.setString(7, (jsonNode.get("name").asText() != "") ? jsonNode.get("name").asText() : "");
            stmt.setString(8, jsonNode.get("userowneruuid").asText());
            stmt.setString(9, jsonNode.get("clientdatauuid").asText());
            stmt.setString(10, (jsonNode.get("startdate").asText(new SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date()))));
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.MONTH, 4);
            stmt.setString(11, (jsonNode.get("enddate").asText(new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime()))));
            System.out.println("update = " + stmt.executeUpdate());
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Kunne ikke gemme projekt: "+jsonNode, e);
        }
    }

    public void update(JsonNode jsonNode, String uuid)  {
        logger.debug("Update project: "+jsonNode);

        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("UPDATE project p SET p.active = ?, p.budget = ?, p.customerreference = ?, p.name = ?, p.userowneruuid = ?, p.startdate = ?, p.enddate = ? WHERE p.uuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, jsonNode.get("active").asText());
            stmt.setDouble(2, jsonNode.get("budget").asDouble(0.0));
            stmt.setString(3, jsonNode.get("customerreference").asText());
            stmt.setString(4, jsonNode.get("name").asText());
            stmt.setString(5, jsonNode.get("userowneruuid").asText());
            stmt.setDate(6, new Date(new SimpleDateFormat("yyyy-MM-dd").parse(jsonNode.get("startdate").asText()).getTime()));
            stmt.setDate(7, new Date(new SimpleDateFormat("yyyy-MM-dd").parse(jsonNode.get("enddate").asText()).getTime()));
            stmt.setString(8, jsonNode.get("uuid").asText());
            stmt.executeUpdate();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

/*
        Connection connection = database.getConnection();

        String sql = "UPDATE project p SET ";
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        boolean firstRun = true;
        while(fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            if(entry.getKey().equals("uuid")) continue;
            if(!firstRun) sql += ", ";
            sql += ("p."+entry.getKey() +  " = ?");
            firstRun = false;
        }
        sql += " WHERE p.uuid LIKE ?";
        System.out.println("sql = " + sql);

        PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        fields = jsonNode.fields();
        int i = 1;
        while(fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            if(entry.getKey().equals("uuid")) continue;
            else if(entry.getKey().equals("created")) continue;
            //else if(entry.getKey().equals("budget")) stmt.setDouble(i++, entry.getValue().asDouble());
            //else if(entry.getKey().equals("enddate") || entry.getKey().equals("startdate")));
            else stmt.setString(i++, entry.getValue().asText());
        }
        stmt.setString(i, uuid);
        stmt.executeUpdate();
        stmt.close();
        connection.close();
*/
    }
}
