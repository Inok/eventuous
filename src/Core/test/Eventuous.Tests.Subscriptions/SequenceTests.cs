using Eventuous.Subscriptions.Checkpoints;
using Eventuous.TestHelpers.TUnit.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;

namespace Eventuous.Tests.Subscriptions;

public class SequenceTests {
    public SequenceTests() {
        var factory = new LoggerFactory();
        factory.AddProvider(new TUnitLoggerProvider(LogLevel.Information));
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(factory);
        var provider = services.BuildServiceProvider();
        provider.AddEventuousLogs();
    }

    [Test]
    [MethodDataSource(nameof(TestData))]
    public void ShouldReturnFirstBefore(CommitPositionSequence sequence, CommitPosition expected) {
        var first = sequence.FirstBeforeGap();
        first.ShouldBe(expected);
    }

    [Test]
    public void ShouldWorkForOne() {
        var timestamp = DateTime.Now;
        var sequence  = new CommitPositionSequence { new(0, 1, timestamp) };
        sequence.FirstBeforeGap().ShouldBe(new(0, 1, timestamp));
    }

    [Test]
    public void ShouldWorkForRandomGap() {
        var random   = new Random();
        var sequence = new CommitPositionSequence();
        var start    = (ulong)random.Next(1);

        for (var i = start; i < start + 100; i++) {
            sequence.Add(new(i, i, DateTime.Now));
        }

        var gapPlace = random.Next(1, sequence.Count - 1);
        sequence.Remove(sequence.ElementAt(gapPlace));
        sequence.Remove(sequence.ElementAt(gapPlace));

        var first = sequence.FirstBeforeGap();
        first.ShouldBe(sequence.ElementAt(gapPlace - 1));
    }

    [Test]
    public void ShouldWorkForNormalCase() {
        var sequence  = new CommitPositionSequence();
        var timestamp = DateTime.Now;

        for (ulong i = 0; i < 10; i++) {
            sequence.Add(new(i, i, timestamp));
        }

        var first = sequence.FirstBeforeGap();
        first.ShouldBe(new(9, 9, timestamp));
    }

    [Test]
    public void ShouldReturnMaxWhenNoGap() {
        var sequence = Sequence(3, 4, 5);
        sequence.FirstBeforeGap().Sequence.ShouldBe(5UL);
    }

    [Test]
    public void ShouldReturnEmptyForEmptySet() {
        new CommitPositionSequence().FirstBeforeGap().ShouldBe(CommitPosition.None);
    }

    [Test]
    public void ShouldFindGapAtTheStart() {
        var sequence = Sequence(1, 3, 4);
        sequence.FirstBeforeGap().Sequence.ShouldBe(1UL);
    }

    [Test]
    public void ShouldFindGapInTheMiddle() {
        var sequence = Sequence(0, 1, 2, 4, 5);
        sequence.FirstBeforeGap().Sequence.ShouldBe(2UL);
    }

    [Test]
    public void ShouldReturnFirstOfTwoGaps() {
        var sequence = Sequence(0, 1, 3, 4, 6, 7);
        sequence.FirstBeforeGap().Sequence.ShouldBe(1UL);
    }

    [Test]
    public void ShouldWorkForTwoWithoutGap() {
        var sequence = Sequence(7, 8);
        sequence.FirstBeforeGap().Sequence.ShouldBe(8UL);
    }

    [Test]
    public void ShouldWorkForTwoWithGap() {
        var sequence = Sequence(7, 9);
        sequence.FirstBeforeGap().Sequence.ShouldBe(7UL);
    }

    [Test]
    public void RemoveUpTo_removes_exactly_the_prefix() {
        var sequence = Sequence(2, 3, 4, 6, 7);
        sequence.RemoveUpTo(4);
        sequence.Select(x => x.Sequence).ShouldBe([6UL, 7UL]);
    }

    [Test]
    public void RemoveUpTo_removes_across_a_gap() {
        var sequence = Sequence(2, 3, 6, 7);
        sequence.RemoveUpTo(5);
        sequence.Select(x => x.Sequence).ShouldBe([6UL, 7UL]);
    }

    [Test]
    public void RemoveUpTo_is_a_noop_on_empty_set() {
        var sequence = new CommitPositionSequence();
        sequence.RemoveUpTo(10);
        sequence.Count.ShouldBe(0);
    }

    [Test]
    public void RemoveUpTo_below_minimum_removes_nothing() {
        var sequence = Sequence(5, 6, 7);
        sequence.RemoveUpTo(4);
        sequence.Count.ShouldBe(3);
    }

    [Test]
    public void RemoveUpTo_above_maximum_empties_the_set() {
        var sequence = Sequence(5, 6, 7);
        sequence.RemoveUpTo(100);
        sequence.Count.ShouldBe(0);
    }

    static CommitPositionSequence Sequence(params ulong[] sequences) {
        var result = new CommitPositionSequence();

        // Positions differ from sequences so a mix-up between the two shows
        foreach (var seq in sequences) result.Add(new(seq + 100, seq, DateTime.Now));

        return result;
    }

    public static IEnumerable<Func<(CommitPositionSequence, CommitPosition)>> TestData() {
        var timestamp = DateTime.Now;

        yield return () => ([new(0, 1, timestamp), new(0, 2, timestamp), new(0, 4, timestamp), new(0, 6, timestamp)], new(0, 2, timestamp));
        yield return () => ([new(0, 1, timestamp), new(0, 2, timestamp), new(0, 8, timestamp), new(0, 6, timestamp)], new(0, 2, timestamp));
    }
}
