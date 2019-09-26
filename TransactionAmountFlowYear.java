package advanced.transacoes;

import advanced.transacoes.writables.EX8_KEY;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransactionAmountFlowYear {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);


        // criacao do job e seu nome
        Job j = new Job(c, "transaction-amount-flow");

        /*
         *  Registro de classe
         *  Qual é a classe principal que está rodando o main?
         *  -j.setJarByClass(WordCount.class);
         * */
        j.setJarByClass(TransactionAmountFlowYear.class);
        j.setMapperClass(TransactionAmountFlowYear.MapForTransactionAmountFlowYear.class);
        j.setReducerClass(TransactionAmountFlowYear.ReduceForTransactionAmountFlowYear.class);
        j.setCombinerClass(TransactionAmountFlowYear.CombineForTransactionAmountFlowYear.class);


        /*
         * definição dos tipos de saída
         *
         * */
        j.setOutputKeyClass(EX8_KEY.class);
        j.setOutputValueClass(LongWritable.class);



        //definindo numero de reducers.
        j.setNumReduceTasks(2);



        /*
         * Definindo arquivos de entrada e saída
         *
         * */
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    /*
     * Map
     *
     * Mapper<Tipo da chave de entrada, tipo do valor de entrada,
     *         Tipo da chave de saída, Tipo do valor de saída>
     *   Mapper< id arquivo, texto do arquivo, chave, valor>
     *       se for arquivo de texto LongWritable e Text nunca mudam
     *
     * */
    public static class MapForTransactionAmountFlowYear extends Mapper<LongWritable, Text, EX8_KEY, LongWritable> {

        // Funcao de map
        /*
         * Value = cada linha
         *
         *
         * Context é responsável por enviar o map para o reduce
         * */
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //capturando as linhas
            String linha = value.toString();

            //quebrando em palavras
            String[] palavras = linha.split(";");

            //verificando se é o cabeçalho
            if(!palavras[1].equals("year") && !palavras[1].equals("") && !palavras[4].equals("flow") && !palavras[4].equals("")) {
                /*
                *  Chave: composta - ano e fluxo
                *
                * */

                //enviando pro combiner.
                con.write(new EX8_KEY((palavras[1]), (palavras[4])), new LongWritable(1));
            }
        }
    }

    /*
     *
     * Reduce
     *
     * Reducer<Tipo da chave de entrada, tipo do valor de entrada -> saída do map!
     *           Tipo da chave de saída, tipo do valor de saída>
     *          <Palavra, ocorrencias,
     *            ...>
     *
     *
     *
     *
     * */
    public static class CombineForTransactionAmountFlowYear extends Reducer<EX8_KEY, LongWritable, EX8_KEY, LongWritable > {

        // Funcao de reduce
        public void reduce(EX8_KEY key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0;

            //para cada item da lista de valores é feita uma soma
            for(LongWritable v : values){
                sum += v.get();
            }


            //enviando resultado pro reducer.

            con.write(new EX8_KEY(key.getYear(), key.getFlow()), new LongWritable(sum));
        }
    }

    public static class ReduceForTransactionAmountFlowYear extends Reducer<EX8_KEY, LongWritable, Text, LongWritable > {

        // Funcao de reduce
        public void reduce(EX8_KEY key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0;

            //para cada item da lista de valores é feita uma soma
            for(LongWritable v : values){
                sum += v.get();
            }


            //escrevendo no arquivo.

            con.write(new Text(key.getYear()+" "+key.getFlow()), new LongWritable(sum));
        }
    }
}
