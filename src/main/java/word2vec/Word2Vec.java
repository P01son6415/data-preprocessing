package word2vec;

import com.hankcs.hanlp.mining.word2vec.Word2VecTrainer;
import com.hankcs.hanlp.mining.word2vec.WordVectorModel;

import java.io.IOException;

public class Word2Vec {

    public static void main(String[] args) throws IOException {
        Word2VecTrainer trainerBuilder = new Word2VecTrainer();
        WordVectorModel wordVectorModel = new WordVectorModel("paper_vectors.txt");
        System.out.println("==========与“计算机”相关的领域有========");
        System.out.println(wordVectorModel.nearest("计算机")+"\n");
        System.out.println("==========与“人工智能”相关的领域有========");
        System.out.println(wordVectorModel.nearest("人工智能")+"\n");
        System.out.println("==========与“物联网”相关的领域有========");
        System.out.println(wordVectorModel.nearest("物联网")+"\n");
        System.out.println("==========与“机器学习”相关的领域有========");
        System.out.println(wordVectorModel.nearest("")+"\n");

        //wordVectorModelWordVectorModel wordVectorModel = trainerBuilder.train("paper_segment.txt", "paper_vectors.txt");

    }
}
