package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;

import java.util.Map;

public class SyMsListener extends TransactionalMetaStoreEventListener {
    public SyMsListener(Configuration config) {
        super(config);
    }

    @Override
    public void onAlterTable (AlterTableEvent tableEvent) throws MetaException {
        try {
            RawStore msdb = tableEvent.getIHMSHandler().getMS();
            Table tbl = tableEvent.getNewTable();
            Table oldTbl = tableEvent.getOldTable();

            String synapseVerKey = "synapse.version";
            Map<String, String> oldParams = oldTbl.getParameters();
            if (!oldParams.containsKey(synapseVerKey)) oldParams.put(synapseVerKey, "1");
            long newVer = Long.parseLong(oldParams.get(oldParams)) + 1;
            tbl.putToParameters(synapseVerKey, Long.toString(newVer));

            msdb.alterTable(tableEvent.getCatName(), tableEvent.getDbName(), tableEvent.getNewTable().getTableName(), tbl);
        }
        catch (InvalidObjectException e){
            throw new MetaException(
                    "Unable to change table."
                            + " Check metastore logs for detailed stack." + e.getMessage());
        }
    }

}
