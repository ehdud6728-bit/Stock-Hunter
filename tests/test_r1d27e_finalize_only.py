import unittest
from research_formulas.stockhunter_r1d27e_post15_analyze import validate_closeout

def sealed():
    m={'n_frozen':1500,'n_attempted':1500,'n_never_attempted':0,
       'n_complete':1483,'n_quarantined':16,'n_retry_pending':1,'n_unattempted':1}
    old={('event',str(i)): {'source_complete':1,'status':'COMPLETE_VALUE3_POST15_NOT_PIT'} for i in range(1483)}
    old.update({('quarantine',str(i)):{'source_complete':0,'status':'QUARANTINED_NO_IMPUTATION'} for i in range(16)})
    old[('retry','0')]={'source_complete':0,'status':'RETRY_PENDING_PROVIDER_FAILURE'}
    return m,old

class FinalizationSafety(unittest.TestCase):
    def test_sealed_1483_allows_only_analysis(self):
        m,old=sealed();self.assertEqual(validate_closeout(m,old),dict(complete=1483,quarantined=16,retry_missing=1))
    def test_never_attempted_is_not_accepted(self):
        m,old=sealed();m['n_never_attempted']=1
        with self.assertRaisesRegex(RuntimeError,'NEVER_ATTEMPTED'):validate_closeout(m,old)
    def test_retry_missing_never_becomes_zero(self):
        m,old=sealed();old[('retry','0')]['source_complete']=1
        with self.assertRaisesRegex(RuntimeError,'STATUS_INTEGRITY'):validate_closeout(m,old)
    def test_refuse_incorrect_audit_length(self):
        m,old=sealed();old.pop(('retry','0'))
        with self.assertRaisesRegex(RuntimeError,'FULLY_ATTEMPTED'):validate_closeout(m,old)
    def test_quarantine_mismatch_fails(self):
        m,old=sealed();m['n_quarantined']=15
        with self.assertRaisesRegex(RuntimeError,'STATUS_INTEGRITY'):validate_closeout(m,old)
    def test_source_minimum_1200_guard(self):
        m,old=sealed();m['n_complete']=1100;m['n_quarantined']=399
        for i in range(1100,1483):old[('event',str(i))]={'source_complete':0,'status':'QUARANTINED_NO_IMPUTATION'}
        with self.assertRaisesRegex(RuntimeError,'INSUFFICIENT_VALID_POST_FLOW'):validate_closeout(m,old)

if __name__=='__main__':unittest.main()
