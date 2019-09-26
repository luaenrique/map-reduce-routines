package advanced.transacoes.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TransactionsWeightAvgWritable implements Writable {
    private float    quantidade;
    private float    peso;


    public TransactionsWeightAvgWritable(){}


    /*public void set(float value, int n) {
        this.value = value;
        this.n = n;
    }/*

     */


    public TransactionsWeightAvgWritable(float quantidade, float peso) {
        this.quantidade = quantidade;
        this.peso = peso;
    }

    public float getQuantidade() {
        return quantidade;
    }

    public void setQuantidade(float quantidade) {
        this.quantidade = quantidade;
    }

    public float getPeso() {
        return peso;
    }

    public void setPeso(float peso) {
        this.peso = peso;
    }

    //readFields para que os procedimentos leiam
    @Override
    public void readFields(DataInput in) throws IOException {
        this.peso           = Float.parseFloat(in.readUTF());
        this.quantidade     = Float.parseFloat(in.readUTF());
    }


    //metodo de escrita
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(peso));
        out.writeUTF(String.valueOf(quantidade));
    }
}
