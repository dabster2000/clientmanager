package dk.trustworks.clientmanager.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.framework.persistence.GenericRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by hans on 17/03/15.
 */
public class TaskWorkerConstraintBudgetRepository extends GenericRepository {

    private static final Logger log = LogManager.getLogger();

    public TaskWorkerConstraintBudgetRepository() {
        super();
    }

    public List<Map<String, Object>> findByTaskWorkerConstraintUUID(String taskWorkerConstraintUUID) {
        log.debug("TaskWorkerConstraintBudgetRepository.findByTaskWorkerConstraintUUID");
        log.debug("taskWorkerConstraintUUID = " + taskWorkerConstraintUUID);
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("" +
                    "select yt.month, yt.year, yt.created, yt.budget, yt.taskworkerconstraintuuid " +
                    "from taskworkerconstraintbudget yt " +
                    "inner join( " +
                    "select uuid, month, year, taskworkerconstraintuuid, max(created) created " +
                    "from taskworkerconstraintbudget WHERE taskworkerconstraintuuid LIKE ? " +
                    "group by month, year " +
                    ") ss on yt.month = ss.month and yt.year = ss.year and yt.created = ss.created and yt.taskworkerconstraintuuid = ss.taskworkerconstraintuuid;"
                    , ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, taskWorkerConstraintUUID);
            ResultSet resultSet = stmt.executeQuery();
            result = getEntitiesFromResultSet(resultSet);
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            log.error("LOG00220:", e);
        }
        return result;
    }

    public List<Map<String, Object>> findByMonthAndYear(int month, int year) {
        log.debug("TaskWorkerConstraintBudgetRepository.findByMonthAndYear");
        log.debug("month = [" + month + "], year = [" + year + "]");
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("" +
                    "select yt.month, yt.year, yt.created, yt.budget, yt.taskworkerconstraintuuid " +
                    "from taskworkerconstraintbudget yt " +
                    "inner join( " +
                    "select uuid, month, year, taskworkerconstraintuuid, max(created) created " +
                    "from taskworkerconstraintbudget WHERE month = ? and year = ? " +
                    "group by taskworkerconstraintuuid " +
                    ") ss on yt.created = ss.created and yt.taskworkerconstraintuuid = ss.taskworkerconstraintuuid;"
                    , ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setInt(1, month);
            stmt.setInt(2, year);
            ResultSet resultSet = stmt.executeQuery();
            result = getEntitiesFromResultSet(resultSet);
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            log.error("LOG00210:", e);
        }
        return result;
    }

    public List<Map<String, Object>> findByTaskWorkerConstraintUUIDAndMonthAndYearAndDate(String taskWorkerConstraintUUID, int month, int year, Instant ldt) {
        log.debug("TaskWorkerConstraintBudgetRepository.findByTaskWorkerConstraintUUID");
        log.debug("taskWorkerConstraintUUID = [" + taskWorkerConstraintUUID + "], ldt = [" + ldt + "]");
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("" +
                    "select yt.month, yt.year, yt.created, yt.budget, yt.taskworkerconstraintuuid " +
                    "from taskworkerconstraintbudget yt " +
                    "inner join( " +
                    "select uuid, month, year, taskworkerconstraintuuid, max(created) created " +
                    "from taskworkerconstraintbudget WHERE taskworkerconstraintuuid LIKE ? AND created < ? AND month = ? AND year = ?" +
                    "group by month, year " +
                    ") ss on yt.month = ss.month and yt.year = ss.year and yt.created = ss.created and yt.taskworkerconstraintuuid = ss.taskworkerconstraintuuid;"
                    , ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, taskWorkerConstraintUUID);
            stmt.setTimestamp(2, Timestamp.from(ldt));
            stmt.setInt(3, month);
            stmt.setInt(4, year);
            ResultSet resultSet = stmt.executeQuery();
            result = getEntitiesFromResultSet(resultSet);
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            log.error("LOG00230:", e);
        }
        log.entry(result);
        return result;
    }

    public double calculateTotalTaskBudget(String taskUUID) {
        log.debug("TaskWorkerConstraintBudgetRepository.calculateTotalTaskBudget");
        log.debug("taskUUID = [" + taskUUID + "]");

        double result = 0.0;
        try {
            Connection connection = database.getConnection();
            PreparedStatement stmt = connection.prepareStatement("" +
                            "SELECT sum(budget) sum FROM ( " +
                            "select yt.month, yt.year, yt.created, yt.budget, yt.taskworkerconstraintuuid " +
                            "from taskworkerconstraintbudget yt " +
                            "inner join( " +
                            "select uuid, month, year, taskworkerconstraintuuid, max(created) created " +
                            "from taskworkerconstraintbudget WHERE taskworkerconstraintuuid LIKE ? " +
                            "group by month, year " +
                            ") ss on yt.month = ss.month and yt.year = ss.year and yt.created = ss.created and yt.taskworkerconstraintuuid = ss.taskworkerconstraintuuid" +
                            ") we;",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setString(1, taskUUID);
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            result = resultSet.getDouble("sum");
            resultSet.close();
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            log.error("LOG00240:", e);
        }
        log.exit(result);
        return result;
    }

    public void create(JsonNode jsonNode) throws SQLException {
        log.entry(jsonNode);
        log.debug("TaskWorkerConstraintBudgetRepository.create");
        log.debug("jsonNode = [" + jsonNode + "]");
        //testForNull(jsonNode, new String[]{"taskworkerconstraintuuid", "month", "year", "version"});
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("INSERT INTO taskworkerconstraintbudget (uuid, budget, month, year, taskworkerconstraintuuid, created) VALUES (?, ?, ?, ?, ?, ?)", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setString(1, jsonNode.get("uuid").asText(UUID.randomUUID().toString()));
        stmt.setDouble(2, jsonNode.get("budget").asDouble(0.0));
        stmt.setInt(3, jsonNode.get("month").asInt(0));
        stmt.setInt(4, jsonNode.get("year").asInt(0));
        stmt.setString(5, jsonNode.get("taskworkerconstraintuuid").asText());
        stmt.setTimestamp(6, Timestamp.from(Instant.now()));
        stmt.executeUpdate();
        stmt.close();
        connection.close();
        log.exit();
    }

    public void update(JsonNode jsonNode, String uuid) throws SQLException {
        log.entry(jsonNode, uuid);
        log.debug("Update taskworkerconstraintbudget: " + jsonNode);
        log.error("LOG00250: NOT ALLOWED");
        throw new RuntimeException("Not allowed");
        /*
        testForNull(jsonNode, new String[]{"taskWorkerConstraintUUID", "budget"});
        Connection connection = database.getConnection();
        PreparedStatement stmt = connection.prepareStatement("UPDATE taskworkerconstraintbudget SET budget = ?, budgethistory = ? WHERE uuid LIKE ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setDouble(1, (jsonNode.get("budget").asText() != "") ? jsonNode.get("budget").asDouble() : 0.0);
        stmt.setDouble(2, (jsonNode.get("budgetHistory").asText() != "") ? jsonNode.get("budgetHistory").asDouble() : 0.0);
        stmt.setString(3, uuid);
        stmt.executeUpdate();
        stmt.close();
        */
    }
}
