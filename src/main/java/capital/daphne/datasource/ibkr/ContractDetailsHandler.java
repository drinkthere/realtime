package capital.daphne.datasource.ibkr;

import com.ib.client.ContractDetails;

import java.util.List;

public class ContractDetailsHandler implements IbkrController.IContractDetailsHandler {

    @Override
    public void contractDetails(List<ContractDetails> list) {
        for (ContractDetails cd : list) {
            System.out.println(cd);
        }
    }
}
