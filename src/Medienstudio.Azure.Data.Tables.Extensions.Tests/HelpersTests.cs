using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Medienstudio.Azure.Data.Tables.Extensions.Tests
{
    [TestClass]
    public class HelpersTests
    {
        [TestMethod]
        public void TicksKeyTests()
        {
            var ticksKey = Helpers.TicksKey();
            var convertedBack = Helpers.TicksKeyToDateTimeOffset(ticksKey);

            var diff = DateTimeOffset.Now - convertedBack;
            Assert.IsTrue(diff.TotalSeconds < 1);
        }
    }
}