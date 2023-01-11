using System.Text;
using EventBus.Base;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EventBus.AzureServiceBus;

public class EventBusServiceBus : BaseEventBus
{
    private ITopicClient _topicClient;
    private ManagementClient _managementClient;
    private ILogger _logger;


    public EventBusServiceBus(EventBusConfig config, IServiceProvider serviceProvider, ManagementClient managementClient, ITopicClient topicClient, ILogger logger) : base(config, serviceProvider)
    {
        _logger = serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus> ?? throw new InvalidOperationException();
        _managementClient = new ManagementClient(config.EventBusConnectionString);
        _topicClient = CreateTopicClient();
    }

    private ITopicClient CreateTopicClient()
    {
        if (_topicClient is null || _topicClient.IsClosedOrClosing)
        {
            _topicClient = new TopicClient(EventBusConfig.EventBusConnectionString,
                EventBusConfig.DefaultTopicName, RetryPolicy.Default);
        }

        if (!_managementClient.TopicExistsAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult())
            _managementClient.CreateTopicAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult();

        return _topicClient;
    }


    public override void Publish(IntegrationEvent @event)
    {
        var eventName = @event.GetType().Name;

        eventName = ProcessEventName(eventName);

        var eventStr = JsonConvert.SerializeObject(@event);
        var bodyArr = Encoding.UTF8.GetBytes(eventStr);
        var message = new Message()
        {
            MessageId = Guid.NewGuid().ToString(),
            Body = bodyArr,
            Label = eventName
        };
        _topicClient.SendAsync(message).GetAwaiter().GetResult();
    }

    public override void Subscribe<T, TH>()
    {
        var eventName = typeof(T).Name;
        eventName = ProcessEventName(eventName);

        if (!SubsManager.HasSubscriptionForEvent(eventName))
        {
            var subscriptionClient = CreateSubscriptionClientIfNotExists(eventName);

            RegisterSubscriptionClientMessageHandler(subscriptionClient);
        }

        _logger.LogInformation("Subscribing to event {eventName} with {eventHandler}", eventName, typeof(TH).Name);

        SubsManager.AddSubscription<T, TH>();


        throw new NotImplementedException();
    }

    public override void UnSubscribe<T, TH>()
    {
        var eventName = typeof(T).Name;
        try
        {
            var subscriptionClient = CreateSubscriptionClient(eventName);

            subscriptionClient
                .RemoveRuleAsync(eventName)
                .GetAwaiter().GetResult();
        }
        catch (MessagingEntityNotFoundException)
        {
            _logger.LogWarning("The messaging entity {eventName} Could not be found", eventName);

            SubsManager.RemoveSubscription<T, TH>();
        }
        throw new NotImplementedException();
    }

    private void RegisterSubscriptionClientMessageHandler(ISubscriptionClient subscriptionClient)
    {
        subscriptionClient.RegisterMessageHandler(
            async (message, token) =>
            {
                var eventName = $"{message.Label}";
                var messageData = Encoding.UTF8.GetString(message.Body);

                if (await ProcessEvent(ProcessEventName(eventName), messageData))
                    await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);

            },
            new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 10,
                AutoComplete = false
            });

    }

    private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
    {
        var ex = exceptionReceivedEventArgs.Exception;
        var context = exceptionReceivedEventArgs.ExceptionReceivedContext;

        _logger.LogError(ex, "ERROR handling message: {ExceptionMessage} - Context: {@ExceptionContext}", ex.Message, context);

        return Task.CompletedTask;
    }


    private SubscriptionClient CreateSubscriptionClient(string eventName)
    {
        return new SubscriptionClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName,
            GetSubName(eventName));
    }

    private ISubscriptionClient CreateSubscriptionClientIfNotExists(string eventName)
    {
        var subClient = CreateSubscriptionClient(eventName);
        var exists = _managementClient.SubscriptionExistsAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();

        if (!exists)
        {
            _managementClient.CreateSubscriptionAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName))
                .GetAwaiter().GetResult();
            RemoveDefaultRule(subClient);
        }

        CreateRuleIfNotExists(ProcessEventName(eventName), subClient);

        return subClient;

    }

    private void CreateRuleIfNotExists(string eventName, ISubscriptionClient subscriptionClient)
    {
        bool ruleExists;

        try
        {
            var rule = _managementClient.GetRuleAsync(EventBusConfig.DefaultTopicName, eventName, eventName)
                .GetAwaiter().GetResult();

            ruleExists = rule != null;
        }
        catch (MessagingEntityNotFoundException)
        {
            ruleExists = false;
        }

        if (!ruleExists)
        {
            subscriptionClient.AddRuleAsync(new RuleDescription
            {
                Filter = new CorrelationFilter
                {
                    Label = eventName
                },
                Name = eventName
            }).GetAwaiter().GetResult();
        }
    }

    private void RemoveDefaultRule(SubscriptionClient subscriptionClient)
    {
        try
        {
            subscriptionClient
                .RemoveRuleAsync(RuleDescription.DefaultRuleName)
                .GetAwaiter()
                .GetResult();
        }
        catch (MessagingEntityNotFoundException)
        {
            _logger.LogWarning("The messaging entity {DefaultRuleName} Could not be found", RuleDescription.DefaultRuleName);
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        _topicClient.CloseAsync().GetAwaiter().GetResult();
        _managementClient.CloseAsync().GetAwaiter().GetResult();
        _topicClient = null;
        _managementClient = null;
    }
}