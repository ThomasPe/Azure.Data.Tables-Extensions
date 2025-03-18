using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace Medienstudio.Azure.Data.Tables.CSV
{
    internal class BinaryConverter : ByteArrayConverter
    {
        public override string ConvertToString(object value, IWriterRow row, MemberMapData memberMapData)
        {
            return Convert.ToBase64String(value as byte[]);
        }
    }
}
