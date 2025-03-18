using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace Medienstudio.Azure.Data.Tables.CSV;

internal class BinaryConverter : ByteArrayConverter
{
    public override string ConvertToString(object? value, IWriterRow row, MemberMapData memberMapData)
    {
        if (value is byte[] byteArray)
        {
            return Convert.ToBase64String(byteArray);
        }
        return string.Empty;
    }
}
