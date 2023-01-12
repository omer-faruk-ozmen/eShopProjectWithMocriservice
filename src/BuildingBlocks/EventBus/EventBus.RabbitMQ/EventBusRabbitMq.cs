﻿

using System.ComponentModel.Design;
using System.Net.Sockets;
using System.Text;
using EventBus.Base;
using EventBus.Base.Events;
using Newtonsoft.Json;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace EventBus.RabbitMQ
{
    public class EventBusRabbitMq : BaseEventBus
    {
        RabbitMqPersistentConnection _persistentConnection;
        private readonly IConnectionFactory _connectionFactory;
        private readonly IModel consumerChannel;

        public EventBusRabbitMq(EventBusConfig config, IServiceProvider serviceProvider) : base(config, serviceProvider)
        {
            if (config.Connection != null)
            {
                var connJson = JsonConvert.SerializeObject(EventBusConfig.Connection, new JsonSerializerSettings()
                {
                    ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                });
                _connectionFactory = JsonConvert.DeserializeObject<ConnectionFactory>(connJson);
            }
            else
                _connectionFactory = new ConnectionFactory();

            _persistentConnection = new RabbitMqPersistentConnection(_connectionFactory, config.ConnectionRetryCount);

            consumerChannel = CreateConsumerChannel();

            SubsManager.OnEventRemoved += SubsManager_OnEventRemoved;
        }

        private void SubsManager_OnEventRemoved(object? sender, string eventName)
        {
            eventName = ProcessEventName(eventName);

            if (!_persistentConnection.IsConnection)
                _persistentConnection.TryConnect();

            consumerChannel.QueueUnbind(queue: eventName, exchange: EventBusConfig.DefaultTopicName, routingKey: eventName);

            if (SubsManager.IsEmpty)
            {
                consumerChannel.Close();
            }
        }

        public override void Publish(IntegrationEvent @event)
        {
            if (!_persistentConnection.IsConnection)
                _persistentConnection.TryConnect();

            var policy = Policy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .WaitAndRetry(EventBusConfig.ConnectionRetryCount,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (ex, time) =>
                    {
                        //logger
                    });

            var eventName = @event.GetType().Name;
            eventName = ProcessEventName(eventName);

            consumerChannel.ExchangeDeclare(exchange: EventBusConfig.DefaultTopicName, type: "direct");



            var message = JsonConvert.SerializeObject(@event);
            var body = Encoding.UTF8.GetBytes(message);

            policy.Execute(() =>
            {
                var properties = consumerChannel.CreateBasicProperties();
                properties.DeliveryMode = 2;

                consumerChannel.QueueDeclare(queue: GetSubName(eventName), durable: true, exclusive: false,
                    autoDelete: false, arguments: null);

                consumerChannel.BasicPublish(exchange: EventBusConfig.DefaultTopicName, routingKey: eventName, mandatory: true, basicProperties: properties, body: body);
            });


        }

        public override void Subscribe<T, TH>()
        {
            var eventName = typeof(T).Name;
            eventName = ProcessEventName(eventName);

            if (!SubsManager.HasSubscriptionForEvent(eventName))
            {
                if (!_persistentConnection.IsConnection)
                    _persistentConnection.TryConnect();

                consumerChannel.QueueDeclare(queue: GetSubName(eventName), durable: true, exclusive: false,
                    autoDelete: false, arguments: null);

                consumerChannel.QueueBind(queue: GetSubName(eventName), exchange: EventBusConfig.DefaultTopicName, routingKey: eventName);
            }

            SubsManager.AddSubscription<T, TH>();
            StartBasicConsume(eventName);
        }

        public override void UnSubscribe<T, TH>()
        {
            SubsManager.RemoveSubscription<T, TH>();
        }

        private IModel CreateConsumerChannel()
        {
            if (!_persistentConnection.IsConnection)
            {
                _persistentConnection.TryConnect();
            }

            var channel = _persistentConnection.CreateModel();

            channel.ExchangeDeclare(exchange: EventBusConfig.DefaultTopicName, type: "direct");

            return channel;
        }

        private void StartBasicConsume(string eventName)
        {
            if (consumerChannel is not null)
            {
                var consumer = new EventingBasicConsumer(consumerChannel);

                consumer.Received += Consumer_Received;

                consumerChannel.BasicConsume(queue: GetSubName(eventName), autoAck: false, consumer: consumer);
            }
        }

        private async void Consumer_Received(object? sender, BasicDeliverEventArgs e)
        {
            var eventName = e.RoutingKey;

            eventName = ProcessEventName(eventName);

            var message = Encoding.UTF8.GetString(e.Body.Span);

            try
            {
                await ProcessEvent(eventName, message);
            }
            catch (Exception exception)
            {
                //logging
            }
            consumerChannel.BasicAck(e.DeliveryTag, multiple: false);
        }
    }
}
