using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace Medienstudio.Azure.Data.Tables.CSV;

internal class BoolConverter : BooleanConverter
{
    public override string? ConvertToString(object? value, IWriterRow row, MemberMapData memberMapData)
    {
        return value?.ToString()?.ToLower();
    }
}
