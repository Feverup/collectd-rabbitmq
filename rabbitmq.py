"""
python plugin for collectd to obtain rabbitmq stats
"""
import collectd
import urllib2
import urllib
import json
import re

RABBIT_API_URL = "http://{host}:{port}/api/"

PLUGIN_CONFIG = {
    'username': 'guest',
    'password': 'guest',
    'host': 'localhost',
    'port': 15672,
    'realm': 'RabbitMQ Management'
}


def configure(config_values):
    '''
    Load information from configuration file
    '''

    global PLUGIN_CONFIG
    collectd.info('Configuring RabbitMQ Plugin')
    for config_value in config_values.children:
        collectd.info("%s = %s" % (config_value.key,
                                   len(config_value.values) > 0))
        if len(config_value.values) > 0:
            if config_value.key == 'Username':
                PLUGIN_CONFIG['username'] = config_value.values[0]
            elif config_value.key == 'Password':
                PLUGIN_CONFIG['password'] = config_value.values[0]
            elif config_value.key == 'Host':
                PLUGIN_CONFIG['host'] = config_value.values[0]
            elif config_value.key == 'Port':
                PLUGIN_CONFIG['port'] = config_value.values[0]
            elif config_value.key == 'Realm':
                PLUGIN_CONFIG['realm'] = config_value.values[0]
            elif config_value.key == 'Ignore':
                type_rmq = config_value.values[0]
                PLUGIN_CONFIG['ignore'] = {type_rmq: []}
                for regex in config_value.children:
                    PLUGIN_CONFIG['ignore'][type_rmq].append(
                        re.compile(regex.values[0]))


def init():
    '''
    Initalize connection to rabbitmq
    '''
    collectd.info('Initalizing RabbitMQ Plugin')


def get_info(url):
    '''
    return json object from url
    '''

    try:
        info = urllib2.urlopen(url)
    except urllib2.HTTPError as http_error:
        collectd.error("Error: %s" % (http_error))
        return None
    except urllib2.URLError as url_error:
        collectd.error("Error: %s" % (url_error))
        return None
    return json.load(info)


def dispatch_values(values, host, plugin, plugin_instance, metric_type,
                    type_instance=None):
    '''
    dispatch metrics to collectd
    Args:
      values (tuple): the values to dispatch
      host: (str): the name of the vhost
      plugin (str): the name of the plugin. Should be queue/exchange
      plugin_instance (str): the queue/exchange name
      metric_type: (str): the name of metric
      type_instance: Optional
    '''

    collectd.debug("Dispatching %s %s %s %s %s\n\t%s " % (host, plugin,
                   plugin_instance, metric_type, type_instance, values))

    metric = collectd.Values()
    if host:
        metric.host = host
    metric.plugin = plugin
    if plugin_instance:
        metric.plugin_instance = plugin_instance
    metric.type = metric_type
    if type_instance:
        metric.type_instance = type_instance
    metric.values = values
    metric.dispatch()


def want_to_ignore(type_rmq, name):
    """
    Applies ignore regex to the queue.
    """
    if 'ignore' in PLUGIN_CONFIG:
        if type_rmq in PLUGIN_CONFIG['ignore']:
            for regex in PLUGIN_CONFIG['ignore'][type_rmq]:
                match = regex.match(name)
                if match:
                    return True
    return False


def read(input_data=None):
    '''
    reads all metrics from rabbitmq
    '''

    collectd.debug("Reading data with input = %s" % (input_data))
    base_url = RABBIT_API_URL.format(host=PLUGIN_CONFIG['host'],
                                     port=PLUGIN_CONFIG['port'])

    auth_handler = urllib2.HTTPBasicAuthHandler()
    auth_handler.add_password(realm=PLUGIN_CONFIG['realm'],
                              uri=base_url,
                              user=PLUGIN_CONFIG['username'],
                              passwd=PLUGIN_CONFIG['password'])
    opener = urllib2.build_opener(auth_handler)
    urllib2.install_opener(opener)

    overview = get_info("%s/overview" % base_url)
    node_name = overview['node'].split('@')[1]
    #
    object_totals = ['channels', 'connections', 'consumers', 'exchanges', 'queues']
    values = map( overview['object_totals'].get , object_totals )
    dispatch_values(values, node_name, 'overview', None, 'totals')
    #
    queue_totals = ['messages', 'messages_ready', 'messages_unacknowledged']
    values = map( overview['queue_totals'].get , queue_totals )
    dispatch_values(values, node_name, 'overview', None, 'queue_stats')
    #
    queue_totals = map( lambda x : "%s_details" % x , queue_totals )
    values = map( lambda x : overview['queue_totals'][x]['rate'] , queue_totals )
    dispatch_values(values, node_name, 'overview', None, 'queue_stats_details')
    #
    message_stats = ['deliver', 'deliver_get', 'ack', 'deliver_no_ack', 'publish', 'redeliver']
    values = map( overview['message_stats'].get , message_stats )
    dispatch_values(values, node_name, 'overview', None, 'message_stats')
    #
    message_stats = map( lambda x : "%s_details" % x , message_stats )
    values = map( lambda x : overview['message_stats'][x]['rate'] , message_stats )
    dispatch_values(values, node_name, 'overview', None, 'message_stats_details')

    #First get all the nodes
    node_stats = ['disk_free', 'disk_free_limit', 'fd_total',
                  'fd_used', 'mem_limit', 'mem_used',
                  'proc_total', 'proc_used', 'processors', 'run_queue',
                  'sockets_total', 'sockets_used']
    io_stats = [ 'io_seek_count', 'io_seek_avg_time', 'io_sync_count', 'io_sync_avg_time',
                 'io_read_bytes', 'io_read_count', 'io_read_avg_time', 'io_write_bytes',
                 'io_write_count', 'io_write_avg_time', 'queue_index_read_count',
                 'queue_index_write_count', 'queue_index_journal_write_count',
                 'mnesia_ram_tx_count', 'mnesia_disk_tx_count',
                 'msg_store_read_count', 'msg_store_write_count']
    for node in get_info("%s/nodes" % (base_url)):
        values = map( node.get , node_stats )
        dispatch_values(values, node['name'].split('@')[1],
                        'rabbitmq', None, 'rabbitmq_node')
        values = map( node.get , io_stats )
        dispatch_values(values, node['name'].split('@')[1],
                        'rabbitmq', None, 'io_stats')
        values =  map( lambda k : node["%s_details"%k]['rate'] , io_stats )
        dispatch_values(values, node['name'].split('@')[1],
                        'rabbitmq', None, 'io_stats_details')

    #Then get all vhost

    for vhost in get_info("%s/vhosts" % (base_url)):

        vhost_name = urllib.quote(vhost['name'], '')
        collectd.debug("Found vhost %s" % vhost['name'])
        vhost_safename = 'rabbitmq_%s' % (vhost['name'].replace('/', 'default'))

        for queue in get_info("%s/queues/%s" % (base_url, vhost_name)):
            queue_name = urllib.quote(queue['name'], '')
            collectd.debug("Found queue %s" % queue['name'])
            if not want_to_ignore("queue", queue_name):
                queue_data = get_info("%s/queues/%s/%s" % (base_url,
                                                           vhost_name,
                                                           queue_name))
                if queue_data is not None:
                    queue_stats = ['memory', 'messages', 'consumers', 'messages', 'messages_ready', 'messages_unacknowledged']
                    values = map( queue_data.get , queue_stats )
                    dispatch_values(values, vhost_safename, 'queue', queue_data['name'],
                                    'rabbitmq_queue')
                    queue_details = map( lambda x : x + "_details" , ['messages', 'messages', 'messages_ready', 'messages_unacknowledged'] )
                    values_details =  map( lambda x : x['rate'] , map( queue_data.get , queue_details ) )
                    dispatch_values(values_details, vhost_safename, 'queue', queue_data['name'],
                                    'rabbitmq_queue_details')
                else:
                    collectd.warning("Cannot get data back from %s/%s queue" %
                                    (vhost_name, queue_name))

        for exchange in get_info("%s/exchanges/%s" % (base_url,
                                 vhost_name)):
            if exchange.has_key('message_stats'):
                collectd.debug("Found exchange %s" % exchange['name'])
                message_stats = ['publish_in', 'publish_out']
                values = map( exchange['message_stats'].get , message_stats )
                dispatch_values(values, vhost_safename, 'exchange', exchange['name'],
                                'rabbitmq_exchange')


def shutdown():
    '''
    Shutdown connection to rabbitmq
    '''

    collectd.info('RabbitMQ plugin shutting down')

# Register callbacks
collectd.register_config(configure)
collectd.register_init(init)
collectd.register_read(read)
#collectd.register_write(write)
collectd.register_shutdown(shutdown)
