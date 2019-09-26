package advanced.transacoes.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TransactionsWeightValueAvgWritable implements Writable {
    private float    valor;
    private float    peso;
    private int      n;


    public TransactionsWeightValueAvgWritable(){}


    /*public void set(float value, int n) {
        this.value = value;
        this.n = n;
    }/*

     */


    public TransactionsWeightValueAvgWritable(float valor, float peso, int n) {
        this.valor = valor;
        this.peso = peso;
        this.n    = n;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public float getValor() {
        return valor;
    }

    public void setValor(float valor) {
        this.valor = valor;
    }

    public float getPeso() {
        return peso;
    }

    public void setPeso(float peso) {
        this.peso = peso;
    }

    //reduce lê como do map? como o arquivo lê do reduce?
    @Override
    public void readFields(DataInput in) throws IOException {
        this.valor           = Float.parseFloat(in.readUTF());
        this.peso           = Float.parseFloat(in.readUTF());
        this.n           = Integer.parseInt(in.readUTF());
    }


    //dados estão saindo do map e indo pro reduce ou do reduce para o arquivo
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(valor));
        out.writeUTF(String.valueOf(peso));
        out.writeUTF(String.valueOf(n));
    }
}
