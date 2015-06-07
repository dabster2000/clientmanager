package dk.trustworks.clientmanager.service;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.clientmanager.persistence.TaskWorkerConstraintBudgetRepository;
import dk.trustworks.framework.persistence.GenericRepository;
import dk.trustworks.framework.service.DefaultLocalService;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Created by hans on 17/03/15.
 */
public class TaskWorkerConstraintBudgetService extends DefaultLocalService {

    private TaskWorkerConstraintBudgetRepository taskWorkerConstraintBudgetRepository;

    public TaskWorkerConstraintBudgetService() { taskWorkerConstraintBudgetRepository = new TaskWorkerConstraintBudgetRepository(); }

    public List<Map<String, Object>> findByTaskWorkerConstraintUUID(Map<String, Deque<String>> queryParameters) {
        System.out.println("TaskWorkerConstraintBudgetService.findByTaskWorkerConstraintUUID");
        String taskworkerconstraintuuid = queryParameters.get("taskworkerconstraintuuid").getFirst();
        return taskWorkerConstraintBudgetRepository.findByTaskWorkerConstraintUUID(taskworkerconstraintuuid);
    }

    public List<Map<String, Object>> findByTaskWorkerConstraintUUIDAndDate(Map<String, Deque<String>> queryParameters) {
        System.out.println("TaskWorkerConstraintBudgetService.findByTaskWorkerConstraintUUIDAndDate");
        String taskworkerconstraintuuid = queryParameters.get("taskworkerconstraintuuid").getFirst();
        int month = Integer.parseInt(queryParameters.get("month").getFirst());
        int year = Integer.parseInt(queryParameters.get("year").getFirst());
        Instant instant = Instant.ofEpochMilli(Long.parseLong(queryParameters.get("datetime").getFirst()));
        return taskWorkerConstraintBudgetRepository.findByTaskWorkerConstraintUUIDAndMonthAndYearAndDate(taskworkerconstraintuuid, month, year, instant);
    }

    @Override
    public void create(JsonNode jsonNode) throws SQLException {
        taskWorkerConstraintBudgetRepository.create(jsonNode);
    }

    @Override
    public void update(JsonNode jsonNode, String uuid) throws SQLException {
        taskWorkerConstraintBudgetRepository.update(jsonNode, uuid);
    }

    @Override
    public GenericRepository getGenericRepository() {
        return taskWorkerConstraintBudgetRepository;
    }

    @Override
    public String getResourcePath() {
        return "taskWorkerConstraintBudgets";
    }
}
