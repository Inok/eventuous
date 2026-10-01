using System.Diagnostics.CodeAnalysis;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Consumers;
using Eventuous.Subscriptions.Context;
using Eventuous.TestHelpers.TUnit;
using Eventuous.TestHelpers.TUnit.Logging;
using EventHandler = Eventuous.Subscriptions.EventHandler;

namespace Eventuous.Tests.Subscriptions;

[SuppressMessage("Performance", "CA1822:Mark members as static")]
public class DefaultConsumerTests : IDisposable {
    readonly TestEventListener _listener = new();

    [Test]
    public async Task ShouldFailWhenHandlerNacks() {
        var handler  = new FailingHandler();
        var consumer = new DefaultConsumer([handler]);
        var ctx      = TestContext.CreateContext();

        await consumer.Consume(ctx);

        await Assert.That(ctx.HandlingResults.GetFailureStatus()).IsEqualTo(EventHandlingStatus.Failure);
    }

    [Test]
    public async Task ShouldNackWhenTypedHandlerThrowsSynchronously() {
        var error    = new InvalidOperationException("handler failed");
        var consumer = new DefaultConsumer([new TypedHandler(error)]);
        var ctx      = CreateContext(new Handled());

        await consumer.Consume(ctx);

        await Assert.That(ctx.HasFailed()).IsTrue();
        await Assert.That(ctx.HandlingResults.GetException()).IsSameReferenceAs(error);
    }

    [Test]
    public async Task ShouldIgnoreMessageWithoutTypedHandler() {
        var consumer = new DefaultConsumer([new TypedHandler(new InvalidOperationException())]);
        var ctx      = CreateContext(new NotHandled());

        await consumer.Consume(ctx);

        await Assert.That(ctx.WasIgnored()).IsTrue();
        await Assert.That(ctx.HasFailed()).IsFalse();
    }

    static MessageConsumeContext CreateContext(object message)
        => new("id", "type", "application/json", "stream", 0, 0, 0, 0, DateTime.UtcNow, message, null, "test", CancellationToken.None) {
            LogContext = new("test", new LoggerFactory().AddTUnit(LogLevel.Information))
        };

    public void Dispose() => _listener.Dispose();

    record Handled;

    record NotHandled;

    class TypedHandler : EventHandler {
        // Not async: the exception leaves the handler delegate before it returns a task
        public TypedHandler(Exception error) => On<Handled>(_ => throw error);
    }
}

class FailingHandler : IEventHandler {
    public string DiagnosticName => "TestHandler";

    public ValueTask<EventHandlingStatus> HandleEvent(IMessageConsumeContext context) {
        throw new NotImplementedException();
    }
}
