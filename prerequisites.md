# Workshop Prerequisites

> **In this document:**  
> [[_TOC_]]

Welcome to the **Unlock the Power of Messaging Patterns** workshop, where we focus squarely on mastering key integration patterns through hands-on console labs. Participants will build small, self-contained applications highlighting one pattern at a time, free from configuration overhead. This foundation will make evolving these examples into real-world services far easier.

The primary purpose of this document is to ensure that all participants are prepared with the necessary resources before the workshop begins. Following these prerequisite instructions will allow you to dive straight into the hands-on activities without any setup delays.

---

## Observers Are Welcome!

Attendees are invited to simply watch the demos and follow along without pressure to complete every exercise live. You can sit back, take notes, and absorb the pattern explanations as they unfold. All lab materials and guidance will be provided if you prefer to dive in hands-on.

---

## Overview

This workshop will explore key messaging patterns that are essential for building scalable and resilient applications that can handle asynchronous communication between different components.

In this workshop, you work through a series of console-based labs, each spotlighting a distinct messaging pattern:

- **Pub/Sub:** Send events to a topic and have multiple subscribers react independently.
- **Request/Reply:** Implement synchronous calls over asynchronous queues with correlation identifiers.
- **Completing Consumers:** Distribute workload across several identical receivers for horizontal scaling.
- **Dead Letter Queues:** Safely isolate and reprocess messages that cannot be delivered or processed.
- **Saga:** Coordinate a long-running, distributed transaction via event-driven steps.

Implementation of these patterns will be demonstrated using the following Azure services:

- **Azure Service Bus:** Perfect for implementing messaging queueing and publish-subscribe patterns with advanced features.
- **Azure Storage Queues:** A straightforward, cost-effective option for simple messaging queuing and storage solutions.

By the end of the workshop, you will have hands-on experience with each of these services and understand how to apply messaging patterns to build robust, distributed systems.

> **Note:** While we use Azure services ([actually Azure local emulators](#emulators-and-local-development)), the labs focus on the patterns, not the services used.

---

## Emulators and Local Development

All labs run against local emulators and development tools—no cloud subscription is required. We will use storage and messaging emulators to mimic real services on your machine. This approach keeps the setup lightweight and enables fast feedback loops as you experiment.

---

## Why Set Up Resources in Advance?

Preparing your local environment ahead of time ensures each lab starts smoothly. You will avoid interruptions during the live session and maximize hands-on time. By installing and configuring emulators once, you will focus on pattern implementation rather than troubleshooting infrastructure issues.

---

## Cloning the Workshop Repository

You must clone the workshop repository to your local machine to access the workshop materials. Follow these steps:

1. Ensure you have [Git](https://git-scm.com/) installed on your machine.

2. Open a terminal or PowerShell window:

3. Run the following command:

   ```bash
   git clone https://github.com/TaleLearnCode/UnlockThePowerOfMessagingPatterns.git
   ```

4. Navigate into the cloned repository:

   ```bash
   cd UnlockThePowerOfMessaingPatterns
   ```

5. Verify that you can list the lab folders (e.g., `Lab01_PubSub`, `Lab02_RequestResponse`, etc.).

---

## Tools Installation

Before the workshop, please ensure you have the following tools installed and set up on your local environment:

- **Visual Studio 2022 (Community Edition)** [Preferred]: [Download Visual Studio](https://visualstudio.microsoft.com/vs/community/)
  - Workloads: 
    - .NET desktop development
    - Azure development
    - ASP.NET and web development

- **Visual Studio Code** [Alternative]: [Download Visual Studio Code](https://code.visualstudio.com/)
  - **C# Extension**: [Install C# Extension](https://marketplace.visualstudio.com/items?itemName=ms-dotnettools.csharp)
  - **Azure Functions**: [Install Azure Functions Extension](https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-azurefunctions)

- **PowerShell**: [Download PowerShell](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell)

- **Azurite** (Azure Storage Emulator): [Download Azurite](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite)

- **Docker**: [Windows Install](https://docs.docker.com/desktop/setup/install/windows-install/) | [Mac Install](https://docs.docker.com/desktop/setup/install/mac-install/) | [Linux Install](https://docs.docker.com/desktop/setup/install/linux/)

- **WSL Enablement** (Only for Windows):

  - [Install Windows Subsystem for Linux (WSL)](https://learn.microsoft.com/en-us/windows/wsl/install)
  - [Configure Docker to use WSL](https://docs.docker.com/desktop/features/wsl/#:~:text=Turn%20on%20Docker%20Desktop%20WSL%202%201%20Download,engine%20..%20...%206%20Select%20Apply%20%26%20Restart.)

- **Azure Service Bus Emulator**: [Azure Service Bus Emulator Installer](https://github.com/Azure/azure-service-bus-emulator-installer)

> [!IMPORTANT] 
> The emulator uses a specified JSON file to configure the Service Bus queues, topics, and subscriptions. Replace the `ServiceBus-Emulator\Config\Config.json` file with the [configuration file built for the workshop](config.json). This Config.json is built with all of the queues, topics, and subscriptions that will be used during the workshop.

---

## Conclusion

You are all set! With your local environment configured and the necessary tools installed, you can fully engage in the **Unlock the Power of Messaging Patterns** workshop. Get ready for an informative and hands-on experience that will enhance your skills in building scalable, event-driven architectures.

See you at the workshop!