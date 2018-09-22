<p align="center">
    <h3 align="center">Azure Service Bus Features Overview</h3>
</p>

## Introduction
The purpose of this project is to illustrate, with code, some of the features of Azure Service Bus. 

### Features
 - Built in .NET Core 2.1 / .NET Standard 2.0 
 - Uses the new Azure Service Bus SDK (Microsoft.Azure.ServiceBus)
 - Uses ARM SDK (Microsoft.Azure.Management.ResourceManager) to deploy.

### Projects
**ASB.Common.Core**: This is a .NET standard library that collects some of the types/entities used in the other two (publisher, subscriber) projects. Check its internal readme for details.
**ASB.Publisher.Core**: This is a .NET Core 2.1 console app. It is the publisher agent that adds various messages into the ASB. The console menu will guide on what type of messages and for what feature will be inserted into ASB.  
**ASB.Subscriber.Core**: This is a .NET Core 2.1 console app. It is the subscriber agent that will consume messages from various topics/subscriptions of the ASB.

## List of Service Bus Features 
In this code you will find code illustrations and explanations for the following features:
- Message Batching
- Message Deferral
- Message Geo-Replication
- Message Partitioning
- Creating priority subscriptions
- Sessions
- Session States
- Scheduled Message
- Message browsing

There is an additional feature that I will include in some future builds, which is 
- Archiving and Replay 
> I will be building an extension solution to this, which will Archive all incoming messages to a service bus topic into a Cosmos DB, and build some functionality to replay these messages back into the topic, with some query functionality. 

## How It Works
### Setup
1. [Create a ASB namespace](https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-create-namespace-portal) in Azure portal.
2. Create an Azure Service Principal in order to make the deployment of ASB artifacts work. Powershell scripts for this are in the ASP.Common.Core/Assets folder.
3. Add the results of the two steps above to the App.config file in both Publisher and Subscriber projects.
4. In the App.config file, complete the QueueName entry with the name of primary topic you will use. The ARM deployment code will use this as a trigger. If it already exists in ASB, it will skip creation, otherwise it will create all the ASB artifacts. 

### Usage
You have the option to start the Publisher and Subscriber one after the other and see them in action.
Or, alternatively, you can start them side-by-side (Ctrl-F5) and follow the menu commands.

Enjoy! 

## Contributing & Support

Please use the [Issues](https://github.com/hgjura/azure-service-bus-feature-samples/issues) tab to ask anything or request that new features be illustrated. 