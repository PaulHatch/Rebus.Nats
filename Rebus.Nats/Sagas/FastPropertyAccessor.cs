using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace Rebus.Nats.Sagas;

/// <summary>Property accessor, using compiled expression trees to eliminate reflection overhead.</summary>
internal static class FastPropertyAccessor
{
    private static readonly ConcurrentDictionary<(Type, string), Func<object, object?>> _cache = new();

    /// <summary>
    /// Gets the value of a property (including nested properties using dot notation) from an object.
    /// The first call compiles an optimized accessor delegate that is cached for subsequent calls.
    /// </summary>
    /// <param name="obj">The object to read from.</param>
    /// <param name="propertyPath">The property path (supports dot notation like "Address.City").</param>
    /// <returns>The property value, or null if any intermediate property in the path is null.</returns>
    public static object? GetValue(object obj, string propertyPath)
    {
        if (obj == null)
        {
            throw new ArgumentNullException(nameof(obj));
        }

        if (string.IsNullOrEmpty(propertyPath))
        {
            throw new ArgumentException("Property path cannot be null or empty", nameof(propertyPath));
        }

        var accessor = _cache.GetOrAdd(
            (obj.GetType(), propertyPath),
            static key => CompileAccessor(key.Item1, key.Item2));

        return accessor(obj);
    }

    /// <summary>
    /// Pre-compiles property accessors for a given type and property paths.
    /// Useful for warming up the cache during initialization to avoid first-call compilation overhead.
    /// </summary>
    /// <param name="objectType">The type to compile accessors for.</param>
    /// <param name="propertyPaths">The property paths to pre-compile.</param>
    public static void Warmup(Type objectType, params string[] propertyPaths)
    {
        foreach (var path in propertyPaths)
        {
            _cache.GetOrAdd((objectType, path), static key => CompileAccessor(key.Item1, key.Item2));
        }
    }

    private static Func<object, object?> CompileAccessor(Type objectType, string propertyPath)
    {
        var parameter = Expression.Parameter(typeof(object), "obj");
        Expression current = Expression.Convert(parameter, objectType);

        var span = propertyPath.AsSpan();
        var currentType = objectType;

        while (span.Length > 0)
        {
            var dotIndex = span.IndexOf('.');
            var propertyName = dotIndex >= 0 ? span[..dotIndex] : span;

            var propertyInfo = currentType.GetProperty(
                propertyName.ToString(),
                BindingFlags.Public | BindingFlags.Instance);

            if (propertyInfo == null)
            {
                throw new ArgumentException(
                    $"Property '{propertyName.ToString()}' not found on type '{currentType.Name}'");
            }

            current = Expression.Property(current, propertyInfo);
            currentType = propertyInfo.PropertyType;

            span = dotIndex >= 0 ? span[(dotIndex + 1)..] : ReadOnlySpan<char>.Empty;
        }

        var resultAsObject = Expression.Convert(current, typeof(object));
        var lambda = Expression.Lambda<Func<object, object?>>(resultAsObject, parameter);

        return lambda.Compile();
    }
}
