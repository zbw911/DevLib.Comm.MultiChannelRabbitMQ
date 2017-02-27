using System;

namespace Dev.Common.MultiChannelRabbitMQ
{
    public class RabbitMQClient
    {
        //思路：http://blog.csdn.net/QQ994406030/article/details/53019678

        #region 变量
        /// <summary>
        /// MQ连接(单类型的 RabbitMQ 连接类型)
        /// </summary>
        private static RabbitMQHelper rabbitMQHelper = null;

        /// <summary>
        ///  MQ连接池(多个类型的 RabbitMQ 连接类型)
        /// </summary>
        //private static Dictionary<string, RabbitMQHelper> dictionary = new Dictionary<string, RabbitMQHelper>();

        private static object lockObject = new object();
        #endregion

        #region 静态构造函数
        /// <summary>
        /// 静态构造函数
        /// </summary>
        static RabbitMQClient()
        {
            Init();
        }
        #endregion

        #region 初始化
        /// <summary>
        /// 初始化
        /// </summary>
        private static void Init()
        {
            var sConfig = SafeguardConfigHelper.LoadSafeguardConfig();
            if (sConfig != null)
            {
                if (sConfig.MQ_OneConnectionForChannelSize <= 0)
                {
                    sConfig.MQ_OneConnectionForChannelSize = 50; //默认一个MQ Connection可以创建50个Channel进行数据传输
                }
                rabbitMQHelper = new RabbitMQHelper(sConfig.MQ_Uri, sConfig.MQ_VirtualHost, sConfig.MQ_Exchange, (ExchangeType)Enum.Parse(typeof(ExchangeType), sConfig.MQ_ExchangeType), sConfig.MQ_RoutingKey, sConfig.MQ_QueueName, sConfig.MQ_ExchangeDurable, sConfig.MQ_QueueDurable, sConfig.MQ_QueueMessageTTL, sConfig.MQ_UserName, sConfig.MQ_Pwd, sConfig.MQ_OneConnectionForChannelSize);
            }
        }
        #endregion

        #region 获得RabbitMQHelper

        public static RabbitMQHelper geRabbitMQClient()
        {
            if (rabbitMQHelper == null)
            {
                lock (lockObject)
                {
                    if (rabbitMQHelper == null)
                    {
                        Init();
                    }
                }
            }
            return rabbitMQHelper;
        }

        #endregion

        #region 重置连接
        /// <summary>
        /// 重置连接
        /// </summary>
        public static void Reset()
        {
            Init();
        }
        #endregion
    }
}
