package dk.trustworks.clientmanager.service;

import com.fasterxml.jackson.databind.JsonNode;
import dk.trustworks.clientmanager.persistence.ProjectRepository;
import dk.trustworks.clientmanager.persistence.TaskRepository;
import dk.trustworks.clientmanager.persistence.TaskWorkerConstraintBudgetRepository;
import dk.trustworks.clientmanager.persistence.TaskWorkerConstraintRepository;
import dk.trustworks.framework.persistence.GenericRepository;
import dk.trustworks.framework.service.DefaultLocalService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.StreamSupport;

/**
 * Created by hans on 17/03/15.
 */
public class ProjectBudgetService extends DefaultLocalService {

    private static final Logger log = LogManager.getLogger();

    private ProjectRepository projectRepository;
    private TaskRepository taskRepository;
    private TaskWorkerConstraintRepository taskWorkerConstraintRepository;
    private TaskWorkerConstraintBudgetRepository taskWorkerConstraintBudgetRepository;

    public ProjectBudgetService() {
        projectRepository = new ProjectRepository();
        taskRepository = new TaskRepository();
        taskWorkerConstraintRepository = new TaskWorkerConstraintRepository();
        taskWorkerConstraintBudgetRepository = new TaskWorkerConstraintBudgetRepository();
    }

    public List<Map<String, Object>> findByYear(Map<String, Deque<String>> queryParameters) {
        log.debug("ProjectBudgetService.findByYear");
        log.debug("queryParameters = [" + queryParameters + "]");
        long allTimer = System.currentTimeMillis();
        int year = Integer.parseInt(queryParameters.get("year").getFirst());
        try {
            //RestClient restClient = new RestClient();

            List<Map<String, Object>> projectYearBudgets = new ArrayList<>();
            long allWorkTimer = System.currentTimeMillis();
            //List<Work> allWork = restClient.getRegisteredWorkByYear(year);
            log.debug("Load all work: {}", (System.currentTimeMillis() - allWorkTimer));

            //Map<String, Map<String, Map<Integer, List<Work>>>> orderedWork = new HashMap();

            long timer = System.currentTimeMillis();
            /*
            for (Work work : allWork) {
                if (!orderedWork.containsKey(work.getUserUUID())) orderedWork.put(work.getUserUUID(), new HashMap<>());
                if (!orderedWork.get(work.getUserUUID()).containsKey(work.getTaskUUID()))
                    orderedWork.get(work.getUserUUID()).put(work.getTaskUUID(), new HashMap<>());
                if (!orderedWork.get(work.getUserUUID()).get(work.getTaskUUID()).containsKey(work.getMonth()))
                    orderedWork.get(work.getUserUUID()).get(work.getTaskUUID()).put(work.getMonth(), new ArrayList<>());
                orderedWork.get(work.getUserUUID()).get(work.getTaskUUID()).get(work.getMonth()).add(work);
            }
            log.debug("Ordering: {}", (System.currentTimeMillis() - timer));
            */
            timer = System.currentTimeMillis();
            List<Map<String, Object>> projectsAndTasksAndTaskWorkerConstraints = projectRepository.getAllEntities("project");
            log.debug("projectsAndTasksAndTaskWorkerConstraints: {}", (System.currentTimeMillis() - timer));

            StreamSupport.stream(projectsAndTasksAndTaskWorkerConstraints.spliterator(), true).map((project) -> {
                Map<String, Object> budgetSummary = new HashMap<>();//new ProjectYearEconomy(project.getUUID(), project.getName());
                budgetSummary.put("projectuuid", project.get("uuid"));
                budgetSummary.put("projectname", project.get("name"));
                budgetSummary.put("amount", new double[12]);
                List<Map<String, Object>> tasks = taskRepository.findByProjectUUID((String)project.get("uuid")); //project.getTasks();//restClient.getAllProjectTasks(project.getUUID());
                for (int month = 0; month < 12; month++) {
                    for (Map<String, Object> task : tasks) {
                        for (Map<String, Object> taskWorkerConstraint : taskWorkerConstraintRepository.findByTaskUUID((String)task.get("uuid"))) { // restClient.getTaskWorkerConstraint(task.getUUID()
                            Calendar calendar = Calendar.getInstance();
                            calendar.set(year, month, 1, 0, 0);
                            if (year < 2016 && month < 6) calendar = Calendar.getInstance();
                            if (year >= Calendar.getInstance().get(Calendar.YEAR) && month > Calendar.getInstance().get(Calendar.MONTH))
                                calendar = Calendar.getInstance();
                            long specifiedTime = calendar.toInstant().toEpochMilli();
                            List<Map<String, Object>> budgets = taskWorkerConstraintBudgetRepository.findByTaskWorkerConstraintUUIDAndMonthAndYearAndDate(taskWorkerConstraint.get("uuid").toString(), month, year, calendar.getTime());
                            /*
                            List<Work> filteredWork = new ArrayList<Work>();
                            try {
                                filteredWork.addAll(orderedWork.get(taskWorkerConstraint.getUserUUID()).get(taskWorkerConstraint.getTaskUUID()).get(month));
                            } catch (Exception e) {
                            }
                            for (Work work : filteredWork) {
                                budgetSummary.getActual()[month] += (work.getWorkDuration() * taskWorkerConstraint.getPrice());
                            }
                            */
                            if (budgets.size() > 0) ((double[])budgetSummary.get("amount"))[month] += (double)budgets.get(0).get("budget");
                        }
                    }
                }

                return budgetSummary;

            }).forEach(result -> projectYearBudgets.add(result));
            log.debug("Load all: {}", (System.currentTimeMillis() - allTimer));
            return projectYearBudgets;
        } catch (Exception e) {
            log.error("LOG00840:", e);
        }
        return null;
    }

    @Override
    public void create(JsonNode jsonNode) throws SQLException {
        log.debug("ProjectService.create");
        throw new RuntimeException("Not allowed");
    }

    @Override
    public void update(JsonNode jsonNode, String uuid) throws SQLException {
        log.debug("ProjectService.update");
        throw new RuntimeException("Not allowed");
    }

    @Override
    public GenericRepository getGenericRepository() {
        throw new RuntimeException("Not allowed");
    }

    @Override
    public String getResourcePath() {
        return "projectbudget";
    }
}
