package com.starbucks.eventhub;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;

public class EventHub {

	public void sendEvent(String message) throws ServiceBusException, ExecutionException,
	InterruptedException, IOException {
		ConnectionStringBuilder connStr = new ConnectionStringBuilder(
				Properties.NAMESPACE, Properties.EVENTHUB_NAME,
				Properties.SAS_KEY_NAME, Properties.SAS_KEY);
		byte[] payloadBytes = message.getBytes("UTF-8");
		System.out.println("payloadBytes=  " + payloadBytes);
		EventData sendEvent = new EventData(payloadBytes);
		EventHubClient ehClient = EventHubClient
				.createFromConnectionStringSync(connStr.toString());
		ehClient.sendSync(sendEvent);
		System.out.println("message sent successfull..");

	}

	public void receiveEvent() throws ServiceBusException, ExecutionException,
	InterruptedException, IOException {
		ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder(Properties.NAMESPACE, Properties.EVENTHUB_NAME, Properties.SAS_KEY_NAME, Properties.SAS_KEY);
		EventProcessorHost host = new EventProcessorHost(Properties.EVENTHUB_NAME, Properties.CONSUMER_GROUP_NAME, eventHubConnectionString.toString(), Properties.STORAGE_CONNECTION_STRING);
		System.out.println("Registering host named " + host.getHostName());
		EventProcessorOptions options = new EventProcessorOptions();
		options.setExceptionNotification(new ErrorNotificationHandler());
		try
		{
			host.registerEventProcessor(EventProcessor.class, options).get();
		}
		catch (Exception e)
		{
			System.out.print("Failure while registering: ");
			if (e instanceof ExecutionException)
			{
				Throwable inner = e.getCause();
				System.out.println(inner.toString());
			}
			else
			{
				System.out.println(e.toString());
			}
		}

		System.out.println("Press enter to stop");
		try
		{
			System.in.read();
			host.unregisterEventProcessor();

			System.out.println("Calling forceExecutorShutdown");
			EventProcessorHost.forceExecutorShutdown(120);
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
			e.printStackTrace();
		}

		System.out.println("End of sample");
	}


	public static void main(String[] args) {
		EventHub eventHub=new EventHub();
		try {
			eventHub.sendEvent("SBUX Event Hub Test meaasage");
			//Uncomment below to receiveEvent
			eventHub.receiveEvent();
		} catch (ServiceBusException e) {			
			e.printStackTrace();
		} catch (ExecutionException e) {
			
			e.printStackTrace();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}

}
