using System.Text.Json.Serialization;

namespace ScriptManager;

[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Serialization)]
[JsonSerializable(typeof(List<ScriptHistory>))]
[JsonSerializable(typeof(IEnumerable<ScriptHistory>))]
internal partial class SourceGenerationContext : JsonSerializerContext
{
}