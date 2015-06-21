package dk.trustworks.clientmanager.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.framework.persistence.GenericRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Date;
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
        logger.debug("ProjectRepository.findByOrderByNameAsc");
        try (org.sql2o.Connection con = database.open()) {
            return getEntitiesFromMapSet(con.createQuery("SELECT * FROM project ORDER BY name ASC").executeAndFetchTable().asList());
        } catch (Exception e) {
            logger.error("LOG00350:", e);
        }
        return new ArrayList<>();
        //try {
            //List<Map<String, Object>> result = new ArrayList<>();
            /*
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
        */
    }

    public List<Map<String, Object>> findByActiveTrueOrderByNameAsc() {
        logger.debug("ProjectRepository.findByActiveTrueOrderByNameAsc");
        try (org.sql2o.Connection con = database.open()) {
            return getEntitiesFromMapSet(con.createQuery("SELECT * FROM project WHERE active = TRUE ORDER BY name ASC").executeAndFetchTable().asList());
        } catch (Exception e) {
            logger.error("LOG00360:", e);
        }
        return new ArrayList<>();
/*
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
        return result;*/
    }

    public List<Map<String, Object>> findByClientUUID(String clientUUID) {
        logger.debug("ProjectRepository.findByClientUUID");
        logger.debug("clientUUID = [" + clientUUID + "]");
        try (org.sql2o.Connection con = database.open()) {
            return getEntitiesFromMapSet(con.createQuery("SELECT * FROM project WHERE clientuuid LIKE :clientuuid").addParameter("clientuuid", clientUUID).executeAndFetchTable().asList());
        } catch (Exception e) {
            logger.error("LOG00370:", e);
        }
        return new ArrayList<>();
        /*
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
        */
    }

    public List<Map<String, Object>> findByClientUUIDAndActiveTrue(String clientUUID) {
        logger.debug("ProjectRepository.findByClientUUIDAndActiveTrue");
        logger.debug("clientUUID = [" + clientUUID + "]");
        try (org.sql2o.Connection con = database.open()) {
            return getEntitiesFromMapSet(con.createQuery("SELECT * FROM project WHERE clientuuid LIKE :clientuuid AND active = TRUE").addParameter("clientuuid", clientUUID).executeAndFetchTable().asList());
        } catch (Exception e) {
            logger.error("LOG00380:", e);
        }
        return new ArrayList<>();
        /*
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
        */
    }

    public List<Map<String, Object>> findByClientUUIDOrderByNameAsc(String clientUUID) {
        logger.debug("ProjectRepository.findByClientUUIDOrderByNameAsc");
        logger.debug("clientUUID = [" + clientUUID + "]");
        try (org.sql2o.Connection con = database.open()) {
            return getEntitiesFromMapSet(con.createQuery("SELECT * FROM project WHERE clientuuid LIKE :clientuuid ORDER BY name ASC").addParameter("clientuuid", clientUUID).executeAndFetchTable().asList());
        } catch (Exception e) {
            logger.error("LOG00390:", e);
        }
        return new ArrayList<>();
        /*
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
        */
    }

    public List<Map<String, Object>> findByClientUUIDAndActiveTrueOrderByNameAsc(String clientUUID) {
        logger.debug("ProjectRepository.findByClientUUIDAndActiveTrueOrderByNameAsc");
        logger.debug("clientUUID = [" + clientUUID + "]");
        try (org.sql2o.Connection con = database.open()) {
            return getEntitiesFromMapSet(con.createQuery("SELECT * FROM project WHERE clientuuid LIKE :clientuuid AND active = TRUE ORDER BY name ASC").addParameter("clientuuid", clientUUID).executeAndFetchTable().asList());
        } catch (Exception e) {
            logger.error("LOG00400:", e);
        }
        return new ArrayList<>();
        /*
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
        */
    }

    public void create(JsonNode jsonNode) {
        logger.info("ProjectRepository.create");
        logger.info("jsonNode = [" + jsonNode + "]");
        testForNull(jsonNode, new String[]{"clientuuid", "clientdatauuid"});
        try (org.sql2o.Connection con = database.open()) {
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.MONTH, 4);
            con.createQuery("INSERT INTO project (uuid, active, budget, clientuuid, created, customerreference, name, userowneruuid, clientdatauuid, startdate, enddate) VALUES (:uuid, :active, :budget, :clientuuid, :created, :customerreference, :name, :userowneruuid, :clientdatauuid, :startdate, :enddate)")
                    .addParameter("uuid", jsonNode.get("uuid").asText(UUID.randomUUID().toString()))
                    .addParameter("active", jsonNode.get("active").asBoolean(true))
                    .addParameter("budget", jsonNode.get("budget").asDouble(0.0))
                    .addParameter("clientuuid", jsonNode.get("clientuuid").asText())
                    .addParameter("created", new Date(new java.util.Date().getTime()))
                    .addParameter("customerreference", jsonNode.get("customerreference").asText())
                    .addParameter("name", jsonNode.get("name").asText())
                    .addParameter("userowneruuid", jsonNode.get("userowneruuid").asText())
                    .addParameter("clientdatauuid", jsonNode.get("clientdatauuid").asText())
                    .addParameter("startdate", (jsonNode.get("startdate").asText(new SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date()))))
                    .addParameter("enddate", (jsonNode.get("enddate").asText(new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime()))))
                    .executeUpdate();
        } catch (Exception e) {
            logger.error("LOG00410:", e);
        }
        /*
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
        */
    }

    public void update(JsonNode jsonNode, String uuid)  {
        logger.info("ProjectRepository.update");
        logger.info("jsonNode = [" + jsonNode + "], uuid = [" + uuid + "]");

        try (org.sql2o.Connection con = database.open()) {
            con.createQuery("UPDATE project p SET p.active = :active, p.budget = :budget, p.customerreference = :customerreference, p.name = :name, p.userowneruuid = :userowneruuid, p.startdate = :startdate, p.enddate = :enddate WHERE p.uuid LIKE :uuid")
                    .addParameter("active", jsonNode.get("active").asBoolean(true))
                    .addParameter("budget", jsonNode.get("budget").asDouble(0.0))
                    .addParameter("customerreference", jsonNode.get("customerreference").asText())
                    .addParameter("name", jsonNode.get("name").asText())
                    .addParameter("userowneruuid", jsonNode.get("userowneruuid").asText(""))
                    .addParameter("startdate", new Date(new SimpleDateFormat("yyyy-MM-dd").parse(jsonNode.get("startdate").asText()).getTime()))
                    .addParameter("enddate", new Date(new SimpleDateFormat("yyyy-MM-dd").parse(jsonNode.get("enddate").asText()).getTime()))
                    .addParameter("uuid", jsonNode.get("uuid").asText())
                    .executeUpdate();
        } catch (Exception e) {
            logger.error("LOG00420:", e);
        }
/*
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
