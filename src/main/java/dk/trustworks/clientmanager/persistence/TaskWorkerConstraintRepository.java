package dk.trustworks.clientmanager.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.framework.persistence.GenericRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by hans on 17/03/15.
 */
public class TaskWorkerConstraintRepository extends GenericRepository {

    public TaskWorkerConstraintRepository() {
        super();
    }

    public List<Map<String, Object>> findByTaskUUID(String taskUUID) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM taskworkerconstraint WHERE taskuuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, taskUUID);
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

    public Map<String, Object> findByTaskUUIDAndUserUUID(String taskUUID, String userUUID) {
        System.out.println("TaskWorkerConstraintRepository.findByTaskUUIDAndUserUUID");
        System.out.println("taskUUID = [" + taskUUID + "], userUUID = [" + userUUID + "]");
        Map<String, Object> result = new HashMap<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM taskworkerconstraint WHERE taskuuid LIKE ? AND useruuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, taskUUID);
            stmt.setString(2, userUUID);
            ResultSet resultSet = stmt.executeQuery();
            System.out.println("resultSet.isAfterLast() = " + resultSet.next());
            result = getEntityFromResultSet(resultSet);
            System.out.println("result = " + result);
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public void create(JsonNode jsonNode) throws SQLException {
        System.out.println("Create taskworkerconstraint: "+jsonNode);
        testForNull(jsonNode, new String[]{"taskuuid", "useruuid"});
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("INSERT INTO taskworkerconstraint (uuid, price, taskuuid, useruuid) VALUES (?, ?, ?, ?)", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setString(1, jsonNode.get("uuid").asText(UUID.randomUUID().toString()));
        stmt.setDouble(2, jsonNode.get("price").asDouble(0.0));
        stmt.setString(3, jsonNode.get("taskuuid").asText());
        stmt.setString(4, jsonNode.get("useruuid").asText());
        stmt.executeUpdate();
        stmt.close();
        connection.close();
    }

    public void update(JsonNode jsonNode, String uuid) throws SQLException {
        System.out.println("Update taskworkerconstraint: "+jsonNode);
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("UPDATE taskworkerconstraint SET price = ? WHERE uuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setDouble(1, jsonNode.get("price").asDouble());
        stmt.setString(2, uuid);
        stmt.executeUpdate();
        stmt.close();
        connection.close();
    }
}
