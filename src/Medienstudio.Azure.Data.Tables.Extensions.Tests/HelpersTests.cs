using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Medienstudio.Azure.Data.Tables.Extensions.Tests;

[TestClass]
public class HelpersTests
{
    [TestMethod]
    public void TicksKeyTests()
    {
        string ticksKey = Helpers.TicksKey();
        DateTimeOffset convertedBack = Helpers.TicksKeyToDateTimeOffset(ticksKey);

        TimeSpan diff = DateTimeOffset.Now - convertedBack;
        Assert.IsTrue(diff.TotalSeconds < 1);
    }

    [TestMethod]
    public void SafeKeyTests()
    {
        List<string> keys = new()
        {
            "/path/to/file.md",
            @"/\#?",
            "\n",
            " ",
            // should create a key including forward slash
            "subjects?_d=1"
        };

        foreach (string key in keys)
        {
            string safeKey = Helpers.ToSafeKey(key);
            string convertedBack = Helpers.FromSafeKey(safeKey);

            Assert.AreEqual(key, convertedBack);
            Assert.IsFalse(string.IsNullOrWhiteSpace(safeKey));
            // https://stackoverflow.com/questions/13195143/range-of-valid-character-for-a-base-64-encoding
            Assert.IsFalse(safeKey.Any(c => !(char.IsLetterOrDigit(c) || c == '.' || c == '/' || c == '+' || c == '=')));
        }
    }

    [TestMethod]
    public void StartsWithTests()
    {
        string filter = Helpers.StartsWith("column", "prefix");
        Assert.AreEqual("column ge 'prefix' and column lt 'prefiy'", filter);

        filter = Helpers.StartsWith("column", "prefix/");
        Assert.AreEqual("column ge 'prefix/' and column lt 'prefix0'", filter);

        filter = Helpers.StartsWith("column", "prefix0");
        Assert.AreEqual("column ge 'prefix0' and column lt 'prefix1'", filter);

        filter = Helpers.StartsWith("column", "prefix-");
        Assert.AreEqual("column ge 'prefix-' and column lt 'prefix.'", filter);

        string prefix = "prefix" + char.MaxValue;
        filter = Helpers.StartsWith("column", prefix);
        Assert.AreEqual("column ge '" + prefix + "'", filter);
    }
}
