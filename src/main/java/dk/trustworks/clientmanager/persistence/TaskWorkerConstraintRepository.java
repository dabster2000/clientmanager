package dk.trustworks.clientmanager.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.framework.persistence.GenericRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by hans on 17/03/15.
 */
public class TaskWorkerConstraintRepository extends GenericRepository {

    private static final Logger logger = LogManager.getLogger();

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
            logger.error("LOG00200:", e);
        }
        return result;
    }

    public Map<String, Object> findByTaskUUIDAndUserUUID(String taskUUID, String userUUID) {
        logger.debug("TaskWorkerConstraintRepository.findByTaskUUIDAndUserUUID");
        logger.debug("taskUUID = [" + taskUUID + "], userUUID = [" + userUUID + "]");
        Map<String, Object> result = new HashMap<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM taskworkerconstraint WHERE taskuuid LIKE ? AND useruuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, taskUUID);
            stmt.setString(2, userUUID);
            ResultSet resultSet = stmt.executeQuery();
            logger.debug("resultSet.isAfterLast() = " + resultSet.next());
            result = getEntityFromResultSet(resultSet);
            logger.debug("result = " + result);
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            logger.warn("LOG00190:", e);
            result.put("price", 0.0);
            result.put("taskuuid", taskUUID);
            result.put("useruuid", userUUID);
            result.put("uuid", "");
        }
        return result;
    }

    public void create(JsonNode jsonNode) throws SQLException {
        logger.debug("Create taskworkerconstraint: "+jsonNode);
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
        logger.debug("Update taskworkerconstraint: "+jsonNode);
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("UPDATE taskworkerconstraint SET price = ? WHERE uuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setDouble(1, jsonNode.get("price").asDouble());
        stmt.setString(2, uuid);
        stmt.executeUpdate();
        stmt.close();
        connection.close();
    }
}
