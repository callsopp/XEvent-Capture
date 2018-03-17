using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Data;
using System.Data.Sql;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using Microsoft.SqlServer.Management.XEvent;
using Microsoft.SqlServer.XEvent;
using Microsoft.SqlServer.XEvent.Linq;

namespace XEventCapture
{
    /// <summary>
    /// Main work delegation is handled from ExtendedEventCaptureMonitor 
    /// A single instance is spun up at application startup which periodically checks the control tables for any changes and stops/starts trace monitoring Tasks as appropriate.
    /// </summary>
    class ExtendedEventCaptureMonitor
    {
        private static string _MonitoringServer;
        private static string MonServerConnString = "Data Source=<REPLACE>;Integrated Security=true;Application Name=" + System.Reflection.Assembly.GetExecutingAssembly().GetName().Name + ";initial catalog=XEventTrace";
        private static System.Threading.Timer _CheckForChanges;
        private static TimerCallback _checkforchangesCallback;
        private static object _CheckForChangesLocker = new object();
        private static Dictionary<int, string> _CapturesRunning = new Dictionary<int, string>(); /* use the dictionary to store the session ID and server session name - searching on an int as a key is faster */

        public ExtendedEventCaptureMonitor(string MonitoringServer)
        {
            _MonitoringServer = MonitoringServer;
            MonServerConnString = MonServerConnString.Replace("<REPLACE>", _MonitoringServer);
            _checkforchangesCallback = new TimerCallback(CheckControlForChanges);
            _CheckForChanges = new Timer(_checkforchangesCallback, null, 0, 5000);
        }

        private void CheckControlForChanges(object stateInfo)
        {
            if (Monitor.TryEnter(_CheckForChangesLocker)) /* Stop additional calls being made whilst one is still in process */
            {
                try
                {
                    SqlConnection MonConn = null;
                    DataTable dt = new DataTable();
                    try
                    {
                        MonConn = new SqlConnection(MonServerConnString);
                        MonConn.Open();
                        SqlCommand GetServersToTrace = MonConn.CreateCommand();
                        GetServersToTrace.CommandType = CommandType.StoredProcedure;
                        GetServersToTrace.CommandText = "GetServersToTrace";

                        using (SqlDataAdapter adapter = new SqlDataAdapter())
                        {
                            adapter.SelectCommand = GetServersToTrace;
                            adapter.Fill(dt);
                        }

                    }
                    catch (Exception xyz)
                    {

                    }
                    finally
                    {
                        MonConn.Close();
                    }

                    if (dt.Rows.Count > 0)
                    {
                        if (_CapturesRunning.Count > 0)
                        {
                            /* stop threads before checking for new ones */
                            for (int i = 0; i < dt.Rows.Count; i++)
                            {
                                /* check for any active traces which have had their active flag set to 0 */
                                if (!Convert.ToBoolean(dt.Rows[i]["active"]))
                                {
                                    string ss;
                                    if (_CapturesRunning.TryGetValue(Convert.ToInt32(dt.Rows[i]["sID"]), out ss))
                                    {
                                        _CapturesRunning.Remove(Convert.ToInt32(dt.Rows[i]["sID"]));
                                    }
                                }
                            }
                        }

                        DateTime NextCaptureStartAllowedAt = DateTime.Now;

                        for (int i = 0; i < dt.Rows.Count; i++)
                        {

                            /* check for any active traces which have had their active flag set to 1 */
                            if (Convert.ToBoolean(dt.Rows[i]["active"]))
                            {
                                string ss;
                                if (!_CapturesRunning.TryGetValue(Convert.ToInt32(dt.Rows[i]["sID"]), out ss))
                                {
                                    _CapturesRunning.Add(Convert.ToInt32(dt.Rows[i]["sID"]), (string)dt.Rows[i]["server_name"] + "_" + (string)dt.Rows[i]["session_name"]);
                                    while (DateTime.Now < NextCaptureStartAllowedAt)
                                    {
                                        Thread.Sleep(300);
                                    }

                                    StartCapture(Convert.ToInt32(dt.Rows[i]["sID"]), (string)dt.Rows[i]["server_name"], (string)dt.Rows[i]["session_name"]);
                                    NextCaptureStartAllowedAt = DateTime.Now.AddMilliseconds(1500);

                                }
                                else if (_CapturesRunning.TryGetValue(Convert.ToInt32(dt.Rows[i]["sID"]), out ss) && Convert.ToInt64(dt.Rows[i]["secs_since_last_heartbeat"]) > 10)
                                {

                                    try
                                    {
                                        MonConn = new SqlConnection(MonServerConnString);
                                        MonConn.Open();
                                        SqlCommand ResetSession = MonConn.CreateCommand();
                                        ResetSession.CommandType = CommandType.StoredProcedure;
                                        ResetSession.CommandText = "ResetSession";
                                        SqlParameter sID = new SqlParameter("@sID", SqlDbType.Int);
                                        ResetSession.Parameters.Add(sID);
                                        sID.Value = Convert.ToInt32(dt.Rows[i]["sID"]);
                                        ResetSession.ExecuteNonQuery();
                                    }
                                    catch (Exception xyz)
                                    {
                                    }
                                    finally
                                    {
                                        MonConn.Close();
                                    }

                                    _CapturesRunning.Remove(Convert.ToInt32(dt.Rows[i]["sID"]));
                                }
                            }
                        }
                    }
                }
                finally
                {
                    Monitor.Exit(_CheckForChangesLocker);
                }
            }
        }

        private async void StartCapture(int sID, string server_name, string session_name)
        {
            CaptureExtendedEvents cee = new CaptureExtendedEvents(_MonitoringServer, server_name, session_name);
            await Task.Run(() =>
            {
                cee.StartEventStreamCollection();
            });
            _CapturesRunning.Remove(sID);
        }
    }

    public class EventQueue<T>
    {
        private readonly Queue<T> _queue = new Queue<T>();

        public void Enqueue(T item)
        {
            lock (_queue)
            {
                _queue.Enqueue(item);
                if (_queue.Count == 1)
                    Monitor.PulseAll(_queue);
            }
        }

        public T Dequeue()
        {
            lock (_queue)
            {
                while (_queue.Count == 0)
                    Monitor.Wait(_queue);

                return _queue.Dequeue();
            }
        }

        public int Count
        {
            get {
                    return _queue.Count;
                }
        }

        public void Clear()
        {
            _queue.Clear();
        }
    
    }

    public class QueuedEventInserter
    {
        private EventQueue<XEventItem> eventQueue;
        private object _EventInsertLocker = new object();
        private string _ServerName;
        private string _MonitoringServer;
        private System.Threading.Timer _InsertTimer;
        private TimerCallback _callback;

        public QueuedEventInserter(string MonitoringServer, string ServerName)
        {
            eventQueue = new EventQueue<XEventItem>();
            _ServerName = ServerName;
            _MonitoringServer = MonitoringServer;
            _callback = new TimerCallback(EventInsert);
            _InsertTimer = new Timer(_callback, null, 0, 500);
        }

        ~QueuedEventInserter()
        {
            _InsertTimer.Dispose();
            eventQueue.Clear();
        }

        public void SubscribeToEventStream(CaptureExtendedEvents ves)
        {
            ves.streamEvent += QueueEventItemForInsert;
        }

        private void QueueEventItemForInsert(XEventItem ei, EventArgs ea)
        {
            eventQueue.Enqueue(ei);
        }

        private void Clear()
        {
            eventQueue.Clear();
        }

        private void EventInsert(object stateInfo)
        {
            /* if the inserts are started then dont fire another EventInsert as the process is already looping */
            if (Monitor.TryEnter(_EventInsertLocker))
            {
                try
                {
                    int counter = 0;
                    do
                    {

                        try
                        {
                            SqlCommand QICommand;
                            QICommand = new SqlCommand();
                            QICommand.CommandType = CommandType.StoredProcedure;
                            XEventItem eItem = (XEventItem)eventQueue.Dequeue();
                            counter++;
                            QICommand.CommandText = "dbo.InsertXETraceEvent";
                            QICommand.Parameters.Add("@event_name", SqlDbType.VarChar, 75).Value = eItem.event_name;
                            QICommand.Parameters.Add("@server_name", SqlDbType.VarChar, 30).Value = eItem.server_name;
                            QICommand.Parameters.Add("@event_timestamp", SqlDbType.DateTime2,8).Value = eItem.event_timestamp;
                            QICommand.Parameters.Add("@session_id", SqlDbType.VarChar, 200).Value = eItem.session_id;
                            QICommand.Parameters.Add("@transaction_id", SqlDbType.VarChar, 200).Value = eItem.transaction_id;
                            QICommand.Parameters.Add("@database_name", SqlDbType.VarChar, 255).Value = eItem.database_name;
                            QICommand.Parameters.Add("@nt_username", SqlDbType.VarChar, 40).Value = eItem.nt_username;
                            QICommand.Parameters.Add("@object_name", SqlDbType.VarChar, 255).Value = eItem.object_name;
                            QICommand.Parameters.Add("@duration", SqlDbType.BigInt).Value = eItem.duration;
                            QICommand.Parameters.Add("@statement", SqlDbType.NVarChar).Value = eItem.statement;
                            QICommand.Parameters.Add("@xml_report", SqlDbType.Xml).Value = eItem.xml_report;
                            QICommand.Parameters.Add("@client_hostname", SqlDbType.VarChar, 30).Value = eItem.client_hostname;
                            QICommand.Parameters.Add("@application_name", SqlDbType.VarChar, 255).Value = eItem.application_name;
                            QICommand.Parameters.Add("@cpu_time", SqlDbType.BigInt).Value = eItem.cpu_time;
                            QICommand.Parameters.Add("@physical_reads", SqlDbType.BigInt).Value = eItem.physical_reads;
                            QICommand.Parameters.Add("@logical_reads", SqlDbType.BigInt).Value = eItem.logical_reads;
                            QICommand.Parameters.Add("@writes", SqlDbType.BigInt).Value = eItem.writes;
                            QICommand.Parameters.Add("@row_count", SqlDbType.BigInt).Value = eItem.row_count;
                            QICommand.Parameters.Add("@causality_guid", SqlDbType.VarChar, 36).Value = eItem.causality_guid;
                            QICommand.Parameters.Add("@causality_seq", SqlDbType.BigInt).Value = eItem.causality_seq;
                            QICommand.Parameters.Add("@nest_level", SqlDbType.BigInt).Value = eItem.nest_level;
                            QICommand.Parameters.Add("@wait_type", SqlDbType.NVarChar, 120).Value = eItem.wait_type;
                            QICommand.Parameters.Add("@wait_resource", SqlDbType.NVarChar, 3072).Value = eItem.wait_resource;
                            QICommand.Parameters.Add("@resource_owner_type", SqlDbType.NVarChar, 60).Value = eItem.resource_owner_type;
                            QICommand.Parameters.Add("@lock_mode", SqlDbType.VarChar, 30).Value = eItem.lock_mode;
                            foreach (SqlParameter sp in QICommand.Parameters)
                            {
                                if (sp.Value == "")
                                {
                                    sp.Value = System.DBNull.Value;
                                }
                            }
                            InsertXEventAsync(QICommand); /* Fire and forget - do not hold up the queue */
                        }
                        catch (Exception dQe)
                        {
                            Console.WriteLine("Error in dequeue - possible queue clear: " + dQe.ToString());
                        }
                    } while (Convert.ToInt32(eventQueue.Count) != 0);
                }
                finally
                {
                    Monitor.Exit(_EventInsertLocker);
                }
            }
        }

        private async Task InsertXEventAsync(SqlCommand insCommand)
        {
            using (SqlConnection insConn = new SqlConnection("Server=" + _MonitoringServer + ";Database=XEventTrace;Application Name=XEventInsert-" + _ServerName + ";Trusted_Connection=True;Min Pool Size=10;Max Pool Size=250;"))
            {
                using (insCommand)
                {
                    insCommand.Connection = insConn;
                    insConn.Open();
                    await insCommand.ExecuteNonQueryAsync().ContinueWith(_ => insCommand.Connection.Close());
                }
            }
        }
    }

    public static class TaskExtensions
    {
        public static void SwallowException(this Task task)
        {
            task.ContinueWith(_ => { return; });
        }
    }

    public class XEventItem
    {
        public string event_timestamp { get; set; }
        public string server_name { get; set; }
        public string event_name { get; set; }
        public Int64 session_id { get; set; }
        public Int64 transaction_id { get; set; }
        public string database_name { get; set; }
        public string nt_username { get; set; }
        public string object_name { get; set; }
        public Int64 duration { get; set; }
        public string statement { get; set; }
        public string xml_report { get; set; }
        public string client_hostname { get; set; }
        public string application_name { get; set; }
        public Int64 cpu_time { get; set; }
        public Int64 physical_reads { get; set; }
        public Int64 logical_reads { get; set; }
        public Int64 writes { get; set; }
        public Int64 row_count { get; set; }
        public string causality_guid { get; set; }
        public int causality_seq { get; set; }
        public int nest_level { get; set; }
        public string wait_type { get; set; }
        public string wait_resource { get; set; }
        public string resource_owner_type { get; set; }
        public string lock_mode { get; set; }
    }

    public class CaptureExtendedEvents
    {
        public event StreamEvent streamEvent;
        public EventArgs a;
        public delegate void StreamEvent(XEventItem ei, EventArgs a);

        public event ClearEventQueue clearEvents;
        public EventArgs k;
        public delegate void ClearEventQueue(EventArgs k);

        private string _serverName;
        private string _sessionName;
        private string _monitoringServer;
        private string _xEventConnectionString;
        private string _MonServerWriteConnectionString;
        private QueryableXEventData _xEventDataStream;
        private object _streamLocker = new object();
        private object _keepRunningLocker = new object();
        private System.Threading.Timer _KeepRunningTimer;
        private TimerCallback _keeprunningcallback;
        private int _CapturesInTheLastSecond = 0;
        private bool _StopRequested = false;

        private QueuedEventInserter eQueue;

        public CaptureExtendedEvents(string MonitoringServer, string ServerName, string SessionName)
        {
            _monitoringServer = MonitoringServer;
            _serverName = ServerName;
            _sessionName = SessionName;
            _xEventConnectionString = "Server=" + _serverName + ";Database=master;Application Name=XEventStreamCapture-" + _serverName + ";Integrated Security=True";
            _MonServerWriteConnectionString = "Server=" + _monitoringServer + ";Database=XEventTrace;Application Name=XEventStreamCapture-" + _serverName + ";Integrated Security=True";
            _keeprunningcallback = new TimerCallback(KeepRunningChecker);
            _KeepRunningTimer = new Timer(_keeprunningcallback, null, 0, 1000);
            eQueue = new QueuedEventInserter(_monitoringServer, _serverName);
            eQueue.SubscribeToEventStream(this);
        }

        private void KeepRunningChecker(object stateInfo)
        {
            if (Monitor.TryEnter(_keepRunningLocker))
            {
                SqlConnection MonConn = null;
                try
                {
                    MonConn = new SqlConnection(_MonServerWriteConnectionString);
                    MonConn.Open();
                    SqlCommand GetServersToTrace = MonConn.CreateCommand();
                    GetServersToTrace.CommandType = CommandType.StoredProcedure;
                    GetServersToTrace.CommandText = "GetServersToTrace";
                    SqlParameter server_name = new SqlParameter("@server_name", SqlDbType.VarChar);
                    GetServersToTrace.Parameters.Add(server_name);
                    server_name.Value = _serverName;

                    SqlParameter session_name = new SqlParameter("@session_name", SqlDbType.VarChar);
                    GetServersToTrace.Parameters.Add(session_name);
                    session_name.Value = _sessionName;

                    SqlParameter last_event_count = new SqlParameter("@last_event_count", SqlDbType.VarChar);
                    GetServersToTrace.Parameters.Add(last_event_count);
                    last_event_count.Value = _CapturesInTheLastSecond;


                    using (SqlDataReader reader = GetServersToTrace.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (!Convert.ToBoolean(reader["active"]))
                            {
                                _CapturesInTheLastSecond = 0; /* reset to capture count */
                                _KeepRunningTimer.Dispose();
                                _StopRequested = true;
                                if (_xEventDataStream != null)
                                {
                                    _xEventDataStream.Dispose(); /* reset the stream to force exit */
                                }
                                clearEvents(null);
                            }
                        }
                    }

                    _CapturesInTheLastSecond = 0; /* reset to capture count */

                }
                catch (Exception xyz)
                {
                    Console.WriteLine("Fatal error checking if capture should stop" + xyz.ToString());
                }
                finally
                {
                    MonConn.Close();
                    Monitor.Exit(_keepRunningLocker);
                }
            }
        }

        private bool CreateXEventStreamOnTarget()
        {
            try
            {
                string SessionGeneratedText = String.Empty;
                using (SqlConnection MonServerWriteConnection = new SqlConnection(_MonServerWriteConnectionString))
                {
                    using (SqlCommand GetStartupCommand = MonServerWriteConnection.CreateCommand())
                    {
                        GetStartupCommand.CommandType = CommandType.StoredProcedure;
                        GetStartupCommand.CommandText = "GenerateSessionScriptAtStartup";
                        SqlParameter server_name = new SqlParameter("@server_name", SqlDbType.VarChar);
                        GetStartupCommand.Parameters.Add(server_name);
                        SqlParameter session_name = new SqlParameter("@session_name", SqlDbType.NVarChar);
                        GetStartupCommand.Parameters.Add(session_name);
                        server_name.Value = _serverName;
                        session_name.Value = _sessionName;
                        
                        MonServerWriteConnection.Open();
                        using (SqlDataReader reader = GetStartupCommand.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SessionGeneratedText = (string)reader[0];
                            }
                        }
                    }
                }

                using (SqlConnection TargetServer = new SqlConnection(_xEventConnectionString))
                {
                    using (SqlCommand TargetServerStartupCommand = TargetServer.CreateCommand())
                    {
                        TargetServerStartupCommand.CommandType = CommandType.Text;
                        TargetServerStartupCommand.CommandText = SessionGeneratedText;
                        TargetServer.Open();
                        TargetServerStartupCommand.ExecuteNonQuery();
                    }
                }

                return true;

            }catch(Exception startupException)
            {
                Console.WriteLine(startupException.ToString());
                return false;
            }
        }

        public void StartEventStreamCollection()
        {
            if (Monitor.TryEnter(_streamLocker))
            {
                if (CreateXEventStreamOnTarget())
                {
                    try
                    {
                        if (streamEvent != null)
                        {
                            while (streamEvent != null && !_StopRequested)
                            {
                                try
                                {
                                    using (_xEventDataStream = new QueryableXEventData(_xEventConnectionString, _sessionName, EventStreamSourceOptions.EventStream, EventStreamCacheOptions.DoNotCache))
                                    {
                                        foreach (PublishedEvent _xEvent in _xEventDataStream)
                                        {
                                            string server_name = "";
                                            string event_timestamp = "";
                                            string event_name = "";
                                            Int64 session_id = 0;
                                            Int64 transaction_id = 0;
                                            string database_name = "";
                                            string nt_username = "";
                                            string object_name = "";
                                            Int64 duration = 0;
                                            string statement = "";
                                            string xml_report = "";
                                            string client_hostname = "";
                                            string application_name = "";
                                            Int64 cpu_time = 0;
                                            Int64 physical_reads = 0;
                                            Int64 logical_reads = 0;
                                            Int64 writes = 0;
                                            Int64 row_count = 0;
                                            string causality_guid = "";
                                            int causality_seq = 0;
                                            int nest_level = 0;
                                            string wait_type = "";
                                            string wait_resource = "";
                                            string resource_owner_type = "";
                                            string lock_mode = "";

                                            PublishedEventField ef;
                                            event_name = _xEvent.Name;

                                            if (_xEvent.Fields.TryGetValue("object_name", out ef))
                                            {
                                                object_name = ef.Value.ToString();
                                            }
                                            if (_xEvent.Fields.TryGetValue("duration", out ef))
                                            {
                                                duration = Convert.ToInt64(ef.Value.ToString());
                                            }
                                            if (_xEvent.Fields.TryGetValue("statement", out ef))
                                            {
                                                statement = ef.Value.ToString();
                                            }
                                            if (_xEvent.Fields.TryGetValue("cpu_time", out ef))
                                            {
                                                cpu_time = Convert.ToInt64(ef.Value.ToString());
                                            }
                                            if (_xEvent.Fields.TryGetValue("physical_reads", out ef))
                                            {
                                                physical_reads = Convert.ToInt64(ef.Value.ToString());
                                            }
                                            if (_xEvent.Fields.TryGetValue("logical_reads", out ef))
                                            {
                                                logical_reads = Convert.ToInt64(ef.Value.ToString());
                                            }
                                            if (_xEvent.Fields.TryGetValue("writes", out ef))
                                            {
                                                writes = Convert.ToInt64(ef.Value.ToString());
                                            }
                                            if (_xEvent.Fields.TryGetValue("row_count", out ef))
                                            {
                                                row_count = Convert.ToInt64(ef.Value.ToString());
                                            }
                                            if (_xEvent.Fields.TryGetValue("xml_report", out ef))
                                            {
                                                xml_report = ef.Value.ToString();
                                            }
                                            if (_xEvent.Fields.TryGetValue("nest_level", out ef))
                                            {
                                                nest_level = Convert.ToInt32(ef.Value.ToString());
                                            }
                                            if (_xEvent.Fields.TryGetValue("wait_type", out ef))
                                            {
                                                wait_type = ef.Value.ToString();
                                            }
                                            if (_xEvent.Fields.TryGetValue("wait_resource", out ef))
                                            {
                                                wait_resource = ef.Value.ToString();
                                            }
                                            if (_xEvent.Fields.TryGetValue("blocked_process", out ef))
                                            {
                                                xml_report = ef.Value.ToString();
                                            }
                                            if (_xEvent.Fields.TryGetValue("resource_owner_type", out ef))
                                            {
                                                resource_owner_type = ef.Value.ToString();
                                            }
                                            if (_xEvent.Fields.TryGetValue("lock_mode", out ef))
                                            {
                                                lock_mode = ef.Value.ToString();
                                            }

                                            PublishedAction ss;

                                            if (_xEvent.Actions.TryGetValue("collect_system_time", out ss))
                                            {
                                                event_timestamp = ((DateTimeOffset)ss.Value).ToString("yyyy-MM-dd HH:mm:ss.fffff");
                                            }
                                            if (_xEvent.Actions.TryGetValue("session_id", out ss))
                                            {
                                                session_id = Convert.ToInt64(ss.Value.ToString());
                                            }
                                            if (_xEvent.Actions.TryGetValue("transaction_id", out ss))
                                            {
                                                transaction_id = Convert.ToInt64(ss.Value.ToString());
                                            }
                                            if (_xEvent.Actions.TryGetValue("database_name", out ss))
                                            {
                                                database_name = ss.Value.ToString();
                                            }
                                            if (_xEvent.Actions.TryGetValue("nt_username", out ss))
                                            {
                                                nt_username = ss.Value.ToString();
                                            }
                                            if (_xEvent.Actions.TryGetValue("server_instance_name", out ss))
                                            {
                                                server_name = ss.Value.ToString();
                                            }
                                            if (_xEvent.Actions.TryGetValue("client_hostname", out ss))
                                            {
                                                client_hostname = ss.Value.ToString();
                                            }
                                            if (_xEvent.Actions.TryGetValue("client_app_name", out ss))
                                            {
                                                application_name = ss.Value.ToString();
                                            }
                                            if (_xEvent.Actions.TryGetValue("sql_text", out ss))
                                            {
                                                statement = _xEvent.Actions["sql_text"].Value.ToString();
                                            }
                                            if (_xEvent.Actions.TryGetValue("attach_activity_id", out ss))
                                            {
                                                causality_guid = _xEvent.Actions["attach_activity_id"].Value.ToString();
                                                causality_seq = Convert.ToInt32(_xEvent.Actions["attach_activity_id"].Value.ToString().Split('[')[1].ToString().Replace("]", ""));
                                            }
                                            if (_xEvent.Actions.TryGetValue("attach_activity_id_seq", out ss))
                                            {
                                                causality_seq = Convert.ToInt32(_xEvent.Actions["attach_activity_id_seq"].Value.ToString());
                                            }
                                            if (_xEvent.Actions.TryGetValue("wait_resource", out ss))
                                            {
                                                wait_resource = ss.Value.ToString();
                                            }

                                            _CapturesInTheLastSecond++; /* increase the capture count for this session */


                                            if (object_name != "InsertXETraceEvent" && object_name != "sp_reset_connection"
                                                && (
                                                       (_CapturesInTheLastSecond < 10000 && event_name.Contains("wait")) 
                                                    || (_CapturesInTheLastSecond > 10000 && !event_name.Contains("wait"))
                                                    || _CapturesInTheLastSecond < 10000
                                                    )
                                                )
                                            {
                                                
                                                /* XEvent Collected */
                                                streamEvent(new XEventItem()
                                                                            {
                                                                              server_name = _serverName
                                                                            , event_name = event_name
                                                                            , event_timestamp = event_timestamp
                                                                            , session_id = session_id
                                                                            , transaction_id = transaction_id
                                                                            , database_name = database_name
                                                                            , nt_username = nt_username
                                                                            , object_name = object_name
                                                                            , duration = duration
                                                                            , statement = statement
                                                                            , xml_report = xml_report
                                                                            , client_hostname = client_hostname
                                                                            , application_name = application_name
                                                                            , cpu_time = cpu_time
                                                                            , physical_reads = physical_reads
                                                                            , logical_reads = logical_reads
                                                                            , writes = writes
                                                                            , row_count = row_count
                                                                            , causality_guid = causality_guid
                                                                            , causality_seq = causality_seq
                                                                            , nest_level = nest_level
                                                                            , wait_type = wait_type
                                                                            , wait_resource = wait_resource
                                                                            , resource_owner_type = resource_owner_type
                                                                            , lock_mode = lock_mode
                                                                            }, null);
                                            }
                                        }


                                    }
                                }
                                catch (Exception xEx)
                                {
                                    if (!_StopRequested)
                                    {
                                        Console.WriteLine("Inner xerror:: " + xEx.ToString());
                                        if (xEx.ToString().Contains("The server was not found or was not accessible"))
                                        {
                                            streamEvent = null; /* force exit */
                                            Console.WriteLine("Exit Forced!");
                                        }
                                    }
                                    else
                                    {
                                        Console.WriteLine("Stop requested :: server: " + _serverName);
                                    }
                                }
                            }
                        }
                    }
                    finally
                    {
                        Monitor.Exit(_streamLocker);
                    }
                }
            }
        }

    }
}

