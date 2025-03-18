using System;

namespace Medienstudio.Azure.Data.Tables.CSV;

internal static class Helpers
{
    public static string GetPropertyTypeName(this object value)
    {            
        if (value == null)
        {
            return "";
        }
        
        if (value is byte[])
        {
            return "Binary";
        }
        if (value is DateTimeOffset)
        {
            return "DateTime";
        }

        return value.GetType().Name;
    }
}
