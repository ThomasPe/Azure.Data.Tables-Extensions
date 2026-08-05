using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;
using System.Globalization;

namespace Medienstudio.Azure.Data.Tables.CSV;

internal sealed class UtcDateTimeOffsetConverter : DateTimeOffsetConverter
{
    public override string? ConvertToString(object? value, IWriterRow row, MemberMapData memberMapData)
    {
        return value is DateTimeOffset dateTimeOffset
            ? dateTimeOffset.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFZ", CultureInfo.InvariantCulture)
            : base.ConvertToString(value, row, memberMapData);
    }
}