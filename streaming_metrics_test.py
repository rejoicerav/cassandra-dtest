import urllib.request
import time
import pytest
import logging

from cassandra import ConsistencyLevel
from tools.jmxutils import (JolokiaAgent, enable_jmx_ssl, make_mbean)
from dtest import Tester, create_ks, create_cf
from tools.data import insert_c1c2

logger = logging.getLogger(__name__)

class TestStreamingMetrics(Tester):

    def test_outgoing_repair_bytes(self):
        cluster = self.cluster

        tokens = cluster.balanced_tokens(3)
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        cluster.set_configuration_options(values={'num_tokens': 1})

        cluster.populate(3)
        nodes = cluster.nodelist()

        for i in range(0, len(nodes)):
            nodes[i].set_configuration_options(values={'initial_token': tokens[i]})

        cluster.start()

        session = self.patient_cql_connection(nodes[0])

        create_ks(session, name='ks2', rf=2)

        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'},
                        compaction_strategy='LeveledCompactionStrategy')
        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ALL)

        session_n2 = self.patient_exclusive_cql_connection(nodes[1])
        session_n2.execute("TRUNCATE system.available_ranges;")

        session.execute("ALTER KEYSPACE ks2 WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3};")
        nodes[0].nodetool('repair -full ks2 cf')

        with JolokiaAgent(nodes[0]) as jmx:

            streamimng_metrics_mbean = make_mbean('metrics', type='Streaming', name = 'TotalIncomingBytes')
            total_incoming_bytes = jmx.read_attribute(streamimng_metrics_mbean, 'Count')

            streamimng_metrics_mbean = make_mbean('metrics', type='Streaming', name = 'TotalOutgoingBytes')
            total_outgoing_bytes = jmx.read_attribute(streamimng_metrics_mbean, 'Count')

            streamimng_metrics_mbean = make_mbean('metrics', type='Streaming', name = 'TotalOutgoingRepairBytes')
            total_outgoing_repair_bytes = jmx.read_attribute(streamimng_metrics_mbean, 'Count')

            streamimng_metrics_mbean = make_mbean('metrics', type='Streaming', name = 'TotalOutgoingRepairSSTables')
            total_outgoing_repair_sstables = jmx.read_attribute(streamimng_metrics_mbean, 'Count')

        assert total_incoming_bytes > 0

        assert total_outgoing_bytes > 0

        assert total_outgoing_repair_bytes > 0

        assert total_outgoing_repair_sstables > 0