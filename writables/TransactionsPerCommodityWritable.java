package advanced.transacoes.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TransactionsPerCommodityWritable implements Writable {
    private String commodity;
    private float    quantidade;


    public TransactionsPerCommodityWritable(){}

    public TransactionsPerCommodityWritable(String commodity, float quantidade) {
        this.commodity = commodity;
        this.quantidade = quantidade;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public void setQuantidade(float quantidade) {
        this.quantidade = quantidade;
    }

    /*public void set(float value, int n) {
        this.value = value;
        this.n = n;
    }/*

     */

    public String getCommodity() {
        return this.commodity;
    }

    public float getQuantidade(){
        return this.quantidade;
    }



    //reduce lê como do map? como o arquivo lê do reduce?
    @Override
    public void readFields(DataInput in) throws IOException {
        this.commodity      = in.readUTF();
        this.quantidade     = Float.parseFloat(in.readUTF());
    }


    //dados estão saindo do map e indo pro reduce ou do reduce para o arquivo
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(commodity));
        out.writeUTF(String.valueOf(quantidade));
    }
}
