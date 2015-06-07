package dk.trustworks.clientmanager.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.framework.persistence.GenericRepository;

import javax.sql.DataSource;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by hans on 17/03/15.
 */
public class ProjectRepository extends GenericRepository {

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
        System.out.println("Create project: " + jsonNode);
        testForNull(jsonNode, new String[]{"clientuuid", "clientdatauuid"});
        System.out.println("ProjectRepository.create");
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("INSERT INTO project (uuid, active, budget, clientuuid, created, customerreference, name, userowneruuid, clientdatauuid, startDate, endDate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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

    public void update(JsonNode jsonNode, String uuid) throws SQLException {
        System.out.println("Update project: "+jsonNode);
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
            stmt.setString(i++, entry.getValue().asText());
        }
        stmt.setString(i, uuid);
        stmt.executeUpdate();
        stmt.close();
        connection.close();
    }
}
