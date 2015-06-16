package dk.trustworks.clientmanager.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.framework.persistence.GenericRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by hans on 17/03/15.
 */
public class ClientRepository extends GenericRepository {

    private static final Logger logger = LogManager.getLogger();

    public ClientRepository() {
        super();
    }

    public List<Map<String, Object>> findByActiveTrue() {
        logger.debug("client.findByActiveTrue()");
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM client WHERE active = TRUE ORDER BY name ASC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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
        logger.debug("ClientRepository.findByActiveTrueOrderByNameAsc");
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM client WHERE active = TRUE ORDER BY name ASC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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
        logger.debug("Create client: "+jsonNode);
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("INSERT INTO client (uuid, active, contactname, created, name) VALUES (?, ?, ?, ?, ?)", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if(jsonNode.get("uuid").asText() != "") stmt.setString(1, jsonNode.get("uuid").asText());
        else stmt.setString(1, UUID.randomUUID().toString());
        stmt.setBoolean(2, true);
        stmt.setString(3, jsonNode.get("contactname").asText());
        stmt.setDate(4, new Date(new java.util.Date().getTime()));
        stmt.setString(5, jsonNode.get("name").asText());
        stmt.executeUpdate();
        stmt.close();
        connection.close();
    }

    public void update(JsonNode jsonNode, String uuid) throws SQLException {
        logger.debug("Update client: "+jsonNode);
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("UPDATE client SET active = ?, contactname = ?, name = ? WHERE uuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setBoolean(1, jsonNode.get("active").asBoolean());
        stmt.setString(2, jsonNode.get("contactname").asText());
        stmt.setString(3, jsonNode.get("name").asText());
        stmt.setString(4, uuid);
        stmt.executeUpdate();
        stmt.close();
        connection.close();
    }
}
