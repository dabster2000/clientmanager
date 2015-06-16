package dk.trustworks.clientmanager.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.framework.persistence.GenericRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by hans on 17/03/15.
 */
public class ClientDataRepository extends GenericRepository {

    private static final Logger logger = LogManager.getLogger();

    public ClientDataRepository() {
        super();
    }

    public List<Map<String, Object>> findByClientUUID(String clientUUID) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM clientdata WHERE clientuuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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

    public void create(JsonNode jsonNode) throws SQLException {
        logger.debug("ClientDataRepository.create");
        logger.debug("jsonNode = [" + jsonNode + "]");
        testForNull(jsonNode, new String[]{"clientuuid", "clientname"});
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("INSERT INTO clientdata (uuid, city, clientuuid, clientname, contactperson, cvr, ean, otheraddressinfo, postalcode, streetnamenumber) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setString(1, (jsonNode.get("uuid").asText() != "") ? jsonNode.get("uuid").asText() : UUID.randomUUID().toString());
        stmt.setString(2, (jsonNode.get("city").asText() != "") ? jsonNode.get("city").asText() : "");
        stmt.setString(3, jsonNode.get("clientuuid").asText());
        stmt.setString(4, jsonNode.get("clientname").asText());
        stmt.setString(5, (jsonNode.get("contactperson").asText() != "") ? jsonNode.get("contactperson").asText() : "");
        stmt.setString(6, (jsonNode.get("cvr").asText() != "") ? jsonNode.get("cvr").asText() : "");
        stmt.setString(7, (jsonNode.get("ean").asText() != "") ? jsonNode.get("ean").asText() : "");
        stmt.setString(8, (jsonNode.get("otheraddressinfo").asText() != "") ? jsonNode.get("otheraddressinfo").asText() : "");
        stmt.setInt(9, (jsonNode.get("postalcode").asText() != "") ? jsonNode.get("postalcode").asInt() : 0);
        stmt.setString(10, (jsonNode.get("streetnamenumber").asText() != "") ? jsonNode.get("streetnamenumber").asText() : "");
        stmt.executeUpdate();
        stmt.close();
        connection.close();
    }

    public void update(JsonNode jsonNode, String uuid) throws SQLException {
        logger.debug("ClientDataRepository.update");
        logger.debug("jsonNode = [" + jsonNode + "], uuid = [" + uuid + "]");
        // TODO: This does not work!
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("UPDATE clientdata SET city = ?, clientname = ?, contactperson = ?, cvr = ?, ean = ?, otheraddressinfo = ?, postalcode = ?, streetnamenumber  = ? WHERE uuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setString(1, (jsonNode.get("city").asText() != "") ? jsonNode.get("city").asText() : "");
        stmt.setString(2, jsonNode.get("clientname").asText());
        stmt.setString(3, (jsonNode.get("contactperson").asText() != "") ? jsonNode.get("contactperson").asText() : "");
        stmt.setString(4, (jsonNode.get("cvr").asText() != "") ? jsonNode.get("cvr").asText() : "");
        stmt.setString(5, (jsonNode.get("ean").asText() != "") ? jsonNode.get("ean").asText() : "");
        stmt.setString(6, (jsonNode.get("otheraddressinfo").asText() != "") ? jsonNode.get("otheraddressinfo").asText() : "");
        stmt.setInt(7, (jsonNode.get("postalCode").asText() != "") ? jsonNode.get("postalCode").asInt() : 0);
        stmt.setString(8, (jsonNode.get("streetnamenumber").asText() != "") ? jsonNode.get("streetnamenumber").asText() : "");
        stmt.setString(9, uuid);
        stmt.executeUpdate();
        stmt.close();
        connection.close();
    }
}
