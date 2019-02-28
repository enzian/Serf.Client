using MessagePack;

namespace Serf.Client
{

    [MessagePackObject]
    public struct ResponseHeader
    {
        [Key("Seq")]
        public ulong Seq;

        [Key("Error")]
        public string Error;
    }
}
