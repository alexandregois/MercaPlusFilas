﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MercaPlusFilas.Model
{
    public class MessageModel
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string Conteudo { get; set; } = string.Empty;
        public DateTime CriadoEm { get; set; } = DateTime.UtcNow;
    }
}
