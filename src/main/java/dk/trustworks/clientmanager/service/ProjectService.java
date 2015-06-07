package dk.trustworks.clientmanager.service;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.clientmanager.persistence.ProjectRepository;
import dk.trustworks.framework.persistence.GenericRepository;
import dk.trustworks.framework.service.DefaultLocalService;

import java.sql.SQLException;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Created by hans on 17/03/15.
 */
public class ProjectService extends DefaultLocalService {

    private ProjectRepository projectRepository;

    public ProjectService() { projectRepository = new ProjectRepository(); }

    public List<Map<String, Object>> findByActiveTrueOrderByNameAsc(Map<String, Deque<String>> queryParameters) {
        return projectRepository.findByActiveTrueOrderByNameAsc();
    }

    public List<Map<String, Object>> findByClientUUID(Map<String, Deque<String>> queryParameters) {
        String clientUUID = queryParameters.get("clientuuid").getFirst();
        return projectRepository.findByClientUUID(clientUUID);
    }

    public List<Map<String, Object>> findByClientUUIDAndActiveTrue(Map<String, Deque<String>> queryParameters) {
        String clientUUID = queryParameters.get("clientuuid").getFirst();
        return projectRepository.findByClientUUIDAndActiveTrue(clientUUID);
    }

    public List<Map<String, Object>> findByClientUUIDAndActiveTrueOrderByNameAsc(Map<String, Deque<String>> queryParameters) {
        String clientUUID = queryParameters.get("clientuuid").getFirst();
        return projectRepository.findByClientUUIDAndActiveTrueOrderByNameAsc(clientUUID);
    }

    public List<Map<String, Object>> findByClientUUIDOrderByNameAsc(Map<String, Deque<String>> queryParameters) {
        String clientUUID = queryParameters.get("clientuuid").getFirst();
        return projectRepository.findByClientUUIDOrderByNameAsc(clientUUID);
    }

    public List<Map<String, Object>> findByOrderByNameAsc(Map<String, Deque<String>> queryParameters) {
        return projectRepository.findByOrderByNameAsc();
    }

    @Override
    public void create(JsonNode jsonNode) throws SQLException {
        System.out.println("ProjectService.create");
        projectRepository.create(jsonNode);
    }

    @Override
    public void update(JsonNode jsonNode, String uuid) throws SQLException {
        System.out.println("ProjectService.update");
        projectRepository.update(jsonNode, uuid);
    }

    @Override
    public GenericRepository getGenericRepository() {
        return projectRepository;
    }

    @Override
    public String getResourcePath() {
        return "project";
    }
}
