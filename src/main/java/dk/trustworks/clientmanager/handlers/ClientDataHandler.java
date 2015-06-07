package dk.trustworks.clientmanager.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.trustworks.clientmanager.service.ClientDataService;
import dk.trustworks.clientmanager.service.ClientService;
import dk.trustworks.framework.server.DefaultHandler;
import dk.trustworks.framework.service.DefaultLocalService;

/**
 * Created by hans on 16/03/15.
 */
public class ClientDataHandler extends DefaultHandler {

    private final ClientDataService clientDataService;

    public ClientDataHandler() {
        super("clientdata");
        this.clientDataService = new ClientDataService();
    }

    @Override
    protected DefaultLocalService getService() {
        return clientDataService;
    }
}
