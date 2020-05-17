use crate::Result;

pub trait Db: Sync + Send {
    type Transaction;

    fn start_transaction(&self) -> Result<Self::Transaction>;
    fn commit_transaction(&self, txn: Self::Transaction) -> Result<Self::Transaction>;
    fn abort_transaction(&self, txn: Self::Transaction) -> Result<Self::Transaction>;

    fn read(
        &self,
        txn: &mut Self::Transaction,
        table: &str,
        key: &str,
        fields: Option<Vec<String>>,
    ) -> Result<Option<Vec<(String, String)>>>;

    fn insert(
        &self,
        txn: &mut Self::Transaction,
        table: &str,
        key: String,
        values: Vec<(String, String)>,
    ) -> Result<()>;

    fn update(
        &self,
        txn: &mut Self::Transaction,
        table: &str,
        key: String,
        values: Vec<(String, String)>,
    ) -> Result<()>;

    fn scan(
        &self,
        txn: &mut Self::Transaction,
        table: &str,
        key: &str,
        length: usize,
        fields: Option<Vec<String>>,
    ) -> Result<Vec<Vec<(String, String)>>>;
}

pub struct MockDb {
    quiet: bool,
}

impl MockDb {
    pub fn new(quiet: bool) -> Self {
        Self { quiet }
    }
}

impl Db for MockDb {
    type Transaction = ();

    fn start_transaction(&self) -> Result<Self::Transaction> {
        if !self.quiet {
            println!("START TRANSACTION");
        }

        Ok(())
    }

    fn commit_transaction(&self, _txn: Self::Transaction) -> Result<Self::Transaction> {
        if !self.quiet {
            println!("COMMIT TRANSACTION");
        }

        Ok(())
    }

    fn abort_transaction(&self, _txn: Self::Transaction) -> Result<Self::Transaction> {
        if !self.quiet {
            println!("ABORT TRANSACTION");
        }

        Ok(())
    }

    fn read(
        &self,
        _txn: &mut Self::Transaction,
        table: &str,
        key: &str,
        fields: Option<Vec<String>>,
    ) -> Result<Option<Vec<(String, String)>>> {
        if !self.quiet {
            println!(
                "READ {} {} {}",
                table,
                key,
                if let Some(fields) = fields {
                    format!("{:?}", fields)
                } else {
                    "<all fields>".to_owned()
                }
            );
        }

        Ok(None)
    }

    fn insert(
        &self,
        _txn: &mut Self::Transaction,
        table: &str,
        key: String,
        values: Vec<(String, String)>,
    ) -> Result<()> {
        if !self.quiet {
            let vals = values
                .iter()
                .map(|(name, val)| format!("{}={}", name, val))
                .collect::<Vec<_>>()
                .join(" ");

            println!("INSERT {} {} [{}]", table, key, vals);
        }

        Ok(())
    }

    fn update(
        &self,
        _txn: &mut Self::Transaction,
        table: &str,
        key: String,
        values: Vec<(String, String)>,
    ) -> Result<()> {
        if !self.quiet {
            let vals = values
                .iter()
                .map(|(name, val)| format!("{}={}", name, val))
                .collect::<Vec<_>>()
                .join(" ");

            println!("UPDATE {} {} [{}]", table, key, vals);
        }

        Ok(())
    }

    fn scan(
        &self,
        _txn: &mut Self::Transaction,
        table: &str,
        key: &str,
        length: usize,
        fields: Option<Vec<String>>,
    ) -> Result<Vec<Vec<(String, String)>>> {
        if !self.quiet {
            println!(
                "SCAN {} {} {} {}",
                table,
                key,
                length,
                if let Some(fields) = fields {
                    format!("{:?}", fields)
                } else {
                    "<all fields>".to_owned()
                }
            );
        }

        Ok(Vec::new())
    }
}
