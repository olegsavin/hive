package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;

public class SyMsListener extends TransactionalMetaStoreEventListener {
    public SyMsListener(Configuration config) {
        super(config);
    }

    @Override
    public void onAlterTable (AlterTableEvent tableEvent) throws MetaException {
        RawStore msdb = tableEvent.getIHMSHandler().getMS();
    }

}
