using EventBus.Base.Abstraction;
using EventBus.Base.Events;

namespace EventBus.Base.SubManagers;

public class InMemoryEventBusSubscriptionManager : IEventBusSubscriptionManager
{
    private readonly Dictionary<string, List<SubscriptionInfo>> _handlers;
    private readonly List<Type> _eventTypes;

    public Func<string, string> EventNameGetter;


    public InMemoryEventBusSubscriptionManager(Func<string, string> eventNameGetter)
    {
        _handlers = new Dictionary<string, List<SubscriptionInfo>>();
        _eventTypes = new List<Type>();
        this.EventNameGetter = eventNameGetter;
    }

    public bool IsEmpty => !_handlers.Keys.Any();
    public event EventHandler<string>? OnEventRemoved;

    public void AddSubscription<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>
    {
        var eventName = GetEventKey<T>();

        AddSubscription(typeof(TH), eventName);

        if (!_eventTypes.Contains(typeof(T)))
            _eventTypes.Add(typeof(T));
    }

    private void AddSubscription(Type handlerType, string eventName)
    {
        if (!HasSubscriptionForEvent(eventName))
            _handlers.Add(eventName, new List<SubscriptionInfo>());
        if (_handlers[eventName].Any(s => s.HandlerType == handlerType))
            throw new ArgumentException($"Handler Type {handlerType.Name} already registered for '{eventName}'",
                nameof(handlerType));

        _handlers[eventName].Add(SubscriptionInfo.Typed(handlerType));
    }

    public void RemoveSubscription<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>
    {
        var handlerToRemove = FindSubscriptionToRemove<T, TH>();
        var eventName = GetEventKey<T>();
        RemoveHandler(eventName, handlerToRemove);
    }

    private void RemoveHandler(string eventName, SubscriptionInfo subsToRemove)
    {
        if (subsToRemove is not null)
        {
            _handlers[eventName].Remove(subsToRemove);

            if (!_handlers[eventName].Any())
            {
                _handlers.Remove(eventName);
                var eventType = _eventTypes.SingleOrDefault(e => e.Name == eventName);
                if (eventType is not null)
                    _eventTypes.Remove(eventType);
                RaiseOnEventRemoved(eventName);
            }
        }
    }

    private void RaiseOnEventRemoved(string eventName)
    {
        var handler = OnEventRemoved;
        handler?.Invoke(this, eventName);
    }
    private SubscriptionInfo FindSubscriptionToRemove<T, TH>() where T : IntegrationEvent
        where TH : IIntegrationEventHandler<T>
    {
        var eventName = GetEventKey<T>();
        return FindSubscriptionToRemove(eventName, typeof(TH));
    }

    private SubscriptionInfo FindSubscriptionToRemove(string eventName, Type handlerType)
    {
        if (!HasSubscriptionForEvent(eventName))
            return null;

        return _handlers[eventName].SingleOrDefault(s => s.HandlerType == handlerType);
    }

    public bool HasSubscriptionForEvent<T>() where T : IntegrationEvent
    {
        var key = GetEventKey<T>();
        return HasSubscriptionForEvent(key);
    }

    public bool HasSubscriptionForEvent(string eventName)
        => _handlers.ContainsKey(eventName);

    public Type GetEventTypeByName(string eventName)
        => _eventTypes.SingleOrDefault(t => t.Name == eventName);

    public void Clear() => _handlers.Clear();

    public IEnumerable<SubscriptionInfo> GetHandlersForEvent<T>() where T : IntegrationEvent
    {
        var key = GetEventKey<T>();
        return GetHandlersForEvent(key);
    }

    public IEnumerable<SubscriptionInfo> GetHandlersForEvent(string eventName)
        => _handlers[eventName];

    public string GetEventKey<T>() where T : IntegrationEvent
    {
        string eventName = typeof(T).Name;
        return EventNameGetter(eventName);
    }
}