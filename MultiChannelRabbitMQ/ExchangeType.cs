using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace APP.API.Async.Common.Safeguard.RabbitMQ
{
    #region ExchangeType
    /// <summary>
    /// 交换机类型
    /// </summary>
    public enum ExchangeType
    {
        /// <summary>
        /// 处理路由键
        /// </summary>
        direct,
        /// <summary>
        /// 不处理路由键
        /// </summary>
        fanout,
        /// <summary>
        /// 将路由键和某模式进行匹配
        /// </summary>
        topic
    }
    #endregion
}
