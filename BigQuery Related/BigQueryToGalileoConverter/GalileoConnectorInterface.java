import java.io.IOException;

import galileo.client.EventPublisher;
import galileo.comm.StorageRequest;
import galileo.dataset.Block;
import galileo.net.ClientMessageRouter;
import galileo.net.NetworkDestination;

abstract class GalileoConnectorInterface {
	private ClientMessageRouter messageRouter;
	private EventPublisher publisher;
	private NetworkDestination server;
	
	public GalileoConnectorInterface(String serverHostName, int serverPort) throws IOException {
		messageRouter = new ClientMessageRouter();
		publisher = new EventPublisher(messageRouter);
		server = new NetworkDestination(serverHostName, serverPort);
	}
	
	public void store(Block fb) throws Exception {
		StorageRequest store = new StorageRequest(fb);
		publisher.publish(server, store);
	}
	
	public void disconnect() {
		messageRouter.shutdown();
	}

}
