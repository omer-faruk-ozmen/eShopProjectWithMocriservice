﻿using Polly;
using RabbitMQ.Client;
using System.Net.Sockets;
using RabbitMQ.Client.Exceptions;

namespace EventBus.RabbitMQ;

public class RabbitMqPersistentConnection : IDisposable
{
    private IConnection _connection;
    private readonly IConnectionFactory _connectionFactory;
    private object lock_object = new object();
    private int retryCount;
    private bool _disposed;

    public RabbitMqPersistentConnection(IConnectionFactory connectionFactory, int retryCount = 5)
    {
        _connectionFactory = connectionFactory;
        this.retryCount = retryCount;
    }

    public bool IsConnection => _connection != null && _connection.IsOpen;

    public IModel CreateModel()
    {
        return _connection.CreateModel();
    }
    public void Dispose()
    {
        _disposed = true;
        _connection.Dispose();
    }

    public bool TryConnect()
    {
        lock (lock_object)
        {
            var policy = Policy.Handle<SocketException>()
                .Or<BrokerUnreachableException>()
                .WaitAndRetry(retryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (ex, time) =>
                    {

                    });

            policy.Execute(() =>
            {
                _connection = _connectionFactory.CreateConnection();
            });

            if (IsConnection)
            {
                _connection.ConnectionShutdown += Connection_ConnectionShutdown;
                _connection.CallbackException += Connection_CallbackException;
                _connection.ConnectionBlocked += Connection_ConnectionBlocked;
                //log
                return true;
            }

            return false;
        }

    }

    private void Connection_ConnectionBlocked(object? sender, global::RabbitMQ.Client.Events.ConnectionBlockedEventArgs e)
    {
        if (_disposed) return;
        TryConnect();
    }

    private void Connection_CallbackException(object? sender, global::RabbitMQ.Client.Events.CallbackExceptionEventArgs e)
    {
        if (_disposed) return;
        TryConnect();
    }

    private void Connection_ConnectionShutdown(object? sender, ShutdownEventArgs e)
    {
        if (_disposed) return;
        //log
        TryConnect();
    }
}