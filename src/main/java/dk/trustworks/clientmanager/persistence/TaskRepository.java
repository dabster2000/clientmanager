package dk.trustworks.clientmanager.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.framework.persistence.GenericRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by hans on 17/03/15.
 */
public class TaskRepository extends GenericRepository {

    private static final Logger logger = LogManager.getLogger();

    public TaskRepository() {
        super();
    }

    public List<Map<String, Object>> findByProjectUUID(String projectUUID) {
        logger.debug("TaskRepository.findByProjectUUID");
        logger.debug("projectUUID = [" + projectUUID + "]");
        try (org.sql2o.Connection con = database.open()) {
            return getEntitiesFromMapSet(con.createQuery("SELECT * FROM task WHERE projectuuid LIKE :projectuuid")
                    .addParameter("projectuuid", projectUUID)
                    .executeAndFetchTable().asList());
        } catch (Exception e) {
            logger.error("LOG00430:", e);
        }
        return new ArrayList<>();
        /*
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
        */
    }

    public List<Map<String, Object>> findByProjectUUIDOrderByNameAsc(String projectUUID) {
        logger.debug("TaskRepository.findByProjectUUIDOrderByNameAsc");
        logger.debug("projectUUID = [" + projectUUID + "]");
        try (org.sql2o.Connection con = database.open()) {
            return getEntitiesFromMapSet(con.createQuery("SELECT * FROM task WHERE projectuuid LIKE :projectuuid ORDER BY name ASC")
                    .addParameter("projectuuid", projectUUID)
                    .executeAndFetchTable().asList());
        } catch (Exception e) {
            logger.error("LOG00440:", e);
        }
        return new ArrayList<>();
        /*
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
        */
    }

    public void create(JsonNode jsonNode) throws SQLException {
        logger.debug("TaskRepository.create");
        logger.debug("jsonNode = [" + jsonNode + "]");
        try (org.sql2o.Connection con = database.open()) {
            con.createQuery("INSERT INTO task (uuid, type, name, projectuuid) VALUES (:uuid, :type, :name, :projectuuid)")
                    .addParameter("uuid", jsonNode.get("uuid").asText(UUID.randomUUID().toString()))
                    .addParameter("type", jsonNode.get("type").asText("KONSULENT"))
                    .addParameter("name", jsonNode.get("name").asText(""))
                    .addParameter("projectuuid", jsonNode.get("projectuuid").asText())
                    .executeUpdate();
        } catch (Exception e) {
            logger.error("LOG00450:", e);
        }
        /*
        logger.debug("Create task: "+jsonNode);
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
        */
    }

    public void update(JsonNode jsonNode, String uuid) throws SQLException {
        logger.debug("TaskRepository.update");
        logger.debug("jsonNode = [" + jsonNode + "], uuid = [" + uuid + "]");
        try (org.sql2o.Connection con = database.open()) {
            con.createQuery("UPDATE task t SET t.type = :type, t.name = :name WHERE t.uuid LIKE :uuid")
                    .addParameter("uuid", jsonNode.get("uuid").asText())
                    .addParameter("type", jsonNode.get("type").asText())
                    .addParameter("name", jsonNode.get("name").asText())
                    .executeUpdate();
        } catch (Exception e) {
            logger.error("LOG00460:", e);
        }
        /*

        logger.debug("Update task: "+jsonNode);
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("UPDATE task t SET t.type = ?, t.name = ? WHERE t.uuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setString(1, jsonNode.get("type").asText("KONSULENT"));
        stmt.setString(2, jsonNode.get("name").asText(""));
        stmt.setString(3, uuid);
        stmt.executeUpdate();
        stmt.close();
        connection.close();
        */
    }
}