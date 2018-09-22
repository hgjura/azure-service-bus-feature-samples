
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

### References
The two projects make use of the following libraries/projects:
- [Serilog](https://serilog.net/). This solution makes for a great logging mechanism. Here I use one of its [many](https://github.com/serilog/serilog/wiki/Provided-Sinks) sinks: [Colored Console](https://github.com/serilog/serilog-sinks-coloredconsole). You can swap this with any of the other sinks for more appropriate log management.
- [CsConsoleFormat](https://github.com/Athari/CsConsoleFormat). This very neat library turns the Window console into a multicolor canvas. Used only to provide for some effects and distinguish butween multiple types of messages/features. 
- [Azure Management Libraries](https://github.com/Azure/azure-libraries-for-net). Fluent edition. Are used here to make for in-line and in-code deployment. This is for illustration purposes only, and to make the code files self contained. In industrial deployments and in your CI/CD pipeline you should use ARM template files and/or Azure CLI commands.

#### Links & other Github repos
- [Official samples repo](https://github.com/Azure/azure-service-bus/tree/master/samples). Big thanks to [@clemensv](https://github.com/clemensv) and his team, for keeping the samples site orderly and up-to-date. This is the full encyclopedia of samples/features of the Service Bus. My project is intended to be a crunched-down version of select features, which I privately use for training and demo purposes.
- [Azure Service Bus](https://azure.microsoft.com/en-us/services/service-bus/) official page. This is where to get started with everything Service Bus.
- [Service Bus Explorer](https://github.com/paolosalvatori/ServiceBusExplorer). The tool _par excellence_ to use and manage Azure Service Bus. Created and tirelessly managed by [Paolo Salvatori](https://github.com/paolosalvatori), whom deserved all the thanks and recognition of all ASB users worldwide. Without this tool, any attempt to manage and debug Service Bus, is simply futile.    
- [Microsoft.Azure.ServiceBus](https://www.nuget.org/packages/Microsoft.Azure.ServiceBus/). The **new** SDK to work with ASB. Greatly different from the **old** SDK (Microsoft.ServiceBus.Messaging), which is not to be used for any future projects, and most likely on its way to being deprecated. The blogosphere is full of blog and samples that use the old SDK. Unfortunately, most are no longer compatible with the new SDK, and the new way of working with Service Bus. Make sure to distinguish it by the way of referencing of the namespace on top of code files. The major give away: the **new** SDK uses ```Message``` object to create ASB messages, the **old** SDK, uses ```MessageBroker```. 



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