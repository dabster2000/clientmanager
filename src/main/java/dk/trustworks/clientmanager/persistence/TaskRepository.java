package dk.trustworks.clientmanager.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.framework.persistence.GenericRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by hans on 17/03/15.
 */
public class TaskRepository extends GenericRepository {

    public TaskRepository() {
        super();
    }

    public List<Map<String, Object>> findByProjectUUID(String projectUUID) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM task WHERE projectuuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, projectUUID);
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

    public List<Map<String, Object>> findByProjectUUIDOrderByNameAsc(String projectUUID) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM task WHERE projectuuid LIKE ? ORDER BY name ASC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, projectUUID);
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

    public void create(JsonNode jsonNode) throws SQLException {
        System.out.println("Create task: "+jsonNode);
        testForNull(jsonNode, new String[]{"projectuuid"});
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("INSERT INTO task (uuid, type, name, projectuuid) VALUES (?, ?, ?, ?)", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setString(1, jsonNode.get("uuid").asText(UUID.randomUUID().toString()));
        stmt.setString(2, jsonNode.get("type").asText("KONSULENT"));
        stmt.setString(3, jsonNode.get("name").asText(""));
        stmt.setString(4, jsonNode.get("projectuuid").asText());
        stmt.executeUpdate();
        stmt.close();
        connection.close();
    }

    public void update(JsonNode jsonNode, String uuid) throws SQLException {
        System.out.println("Update task: "+jsonNode);
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("UPDATE task t SET t.type = ?, t.name = ? WHERE t.uuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setString(1, jsonNode.get("type").asText("KONSULENT"));
        stmt.setString(2, jsonNode.get("name").asText(""));
        stmt.setString(3, uuid);
        stmt.executeUpdate();
        stmt.close();
        connection.close();
    }
}