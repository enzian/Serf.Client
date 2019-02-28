using MessagePack;
using System;
using System.Collections.Generic;
using System.Text;

namespace Serf.Client
{
    [MessagePackObject]
    public class RequestHeader
    {
        [Key("Command")]
        public string Command { get; set; }

        [Key("Seq")]
        public ulong Sequence { get; set; }
    }
}
