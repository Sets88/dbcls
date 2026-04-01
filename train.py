"""
SQL next-word prediction model — training / fine-tuning / inference script.

Imports tokenizer, vocabulary and constants from dbcls.autocomplete.
Uses the same weights.json format as dbcls/autocomplete.py expects.

Usage:
    python train.py train --corpus data.sql                          # train from scratch
    python train.py train --corpus data.sql --finetune               # fine-tune existing weights
    python train.py train --corpus data.sql --epochs 50 --lr 0.005   # custom hyperparams
    python train.py train --corpus data.sql --finetune --weights custom.json --output custom.json
    python train.py infer --sql "SELECT * FROM"                      # run inference
    python train.py infer --sql "SELECT" --top-k 5
"""

import argparse
import json
import math
import random

from dbcls.autocomplete import (
    _tokenize_sql,
    _CONTEXT_LENGTH,
    _WEIGHTS_PATH,
    _VOCABULARY,
    _VOCAB_VALUES,
    _load_weights,
    _predict_next,
)


# ── hyperparameters ────────────────────────────────────────────────────────────

EMBED_DIM     = 5
HIDDEN_SIZE   = 20
EPOCHS        = 20
LEARNING_RATE = 0.01


# ── matrix operations ──────────────────────────────────────────────────────────

def mat_rand(rows: int, cols: int, scale: float = 0.05) -> list:
    return [random.gauss(0, scale) for _ in range(rows * cols)]


def matTvec(matrix: list, rows: int, cols: int, vector: list) -> list:
    output = [0.0] * cols
    for i in range(rows):
        vi = vector[i]
        row_start = i * cols
        for j in range(cols):
            output[j] += matrix[row_start + j] * vi
    return output


def outer_sub(matrix: list, cols: int, a: list, b: list, lr: float):
    for i, ai in enumerate(a):
        grad_scale = lr * ai
        row_start = i * cols
        matrix[row_start: row_start + cols] = [
            matrix[row_start + j] - grad_scale * b[j] for j in range(cols)
        ]


def softmax(logits: list) -> list:
    max_logit = max(logits)
    exp_values = [math.exp(x - max_logit) for x in logits]
    total = sum(exp_values)
    return [x / total for x in exp_values]


# ── model ──────────────────────────────────────────────────────────────────────

class TrainableModel:
    def __init__(self, vocab_size: int, embed_dim: int = EMBED_DIM, hidden_size: int = HIDDEN_SIZE):
        input_dim = _CONTEXT_LENGTH * embed_dim

        self.vocab_size  = vocab_size
        self.embed_dim   = embed_dim
        self.hidden_size = hidden_size
        self.input_dim   = input_dim

        self.embedding_matrix = mat_rand(vocab_size, embed_dim)
        self.hidden_weights   = mat_rand(hidden_size, input_dim)
        self.hidden_bias      = [0.0] * hidden_size
        self.output_weights   = mat_rand(vocab_size, hidden_size)
        self.output_bias      = [0.0] * vocab_size

    def forward(self, context_indices: list):
        input_embeddings = []
        for token_idx in context_indices:
            emb_start = token_idx * self.embed_dim
            input_embeddings.extend(
                self.embedding_matrix[emb_start: emb_start + self.embed_dim]
            )

        pre_hidden = [
            self.hidden_bias[i] + sum(
                self.hidden_weights[i * self.input_dim + j] * input_embeddings[j]
                for j in range(self.input_dim)
            )
            for i in range(self.hidden_size)
        ]
        hidden_activations = [math.tanh(v) for v in pre_hidden]

        logits = [
            self.output_bias[i] + sum(
                self.output_weights[i * self.hidden_size + j] * hidden_activations[j]
                for j in range(self.hidden_size)
            )
            for i in range(self.vocab_size)
        ]
        return input_embeddings, hidden_activations, softmax(logits)

    def step(self, context_indices, input_embeddings, hidden_activations,
             output_probs, target_index, lr):
        output_grad = output_probs[:]
        output_grad[target_index] -= 1.0

        outer_sub(self.output_weights, self.hidden_size, output_grad, hidden_activations, lr)
        for i in range(self.vocab_size):
            self.output_bias[i] -= lr * output_grad[i]

        hidden_grad = matTvec(self.output_weights, self.vocab_size, self.hidden_size, output_grad)
        hidden_grad = [
            hidden_grad[i] * (1.0 - hidden_activations[i] ** 2)
            for i in range(self.hidden_size)
        ]

        outer_sub(self.hidden_weights, self.input_dim, hidden_grad, input_embeddings, lr)
        for i in range(self.hidden_size):
            self.hidden_bias[i] -= lr * hidden_grad[i]

        embedding_grad = matTvec(self.hidden_weights, self.hidden_size, self.input_dim, hidden_grad)
        for k, token_idx in enumerate(context_indices):
            emb_start  = token_idx * self.embed_dim
            grad_start = k * self.embed_dim
            self.embedding_matrix[emb_start: emb_start + self.embed_dim] = [
                self.embedding_matrix[emb_start + e] - lr * embedding_grad[grad_start + e]
                for e in range(self.embed_dim)
            ]

    def predict(self, context_indices: list, top_k: int = 5) -> list:
        _, _, output_probs = self.forward(context_indices)
        sorted_indices = sorted(range(len(output_probs)), key=lambda i: -output_probs[i])
        return [(i, output_probs[i]) for i in sorted_indices[:top_k]]


# ── data helpers ───────────────────────────────────────────────────────────────

def load_corpus(filepath: str) -> list:
    with open(filepath) as fh:
        raw_lines = fh.readlines()
    return [
        line.strip()
        for line in raw_lines
        if line.strip() and not line.strip().startswith('--')
    ]


def encode(tokens: list, token_to_index: dict) -> list:
    unknown_index = 0
    return [token_to_index.get(token, unknown_index) for token in tokens]


def pad_context(indices: list, pad_index: int = 0) -> list:
    return [pad_index] * (_CONTEXT_LENGTH - len(indices)) + indices


def make_dataset(corpus: list, token_to_index: dict, vocab_values: set, debug: bool = False) -> list:
    pad_index = 0
    training_pairs = []
    for sentence in corpus:
        tokenized = _tokenize_sql(sentence, vocab_values)
        if debug:
            print(f"  {sentence}")
            print(f"  → {tokenized}")
        token_indices = encode(tokenized, token_to_index)
        for pos in range(len(token_indices) - 1):
            window_start   = max(0, pos - _CONTEXT_LENGTH + 1)
            context_window = token_indices[window_start: pos + 1]
            padded_context = pad_context(context_window, pad_index)
            target_index   = token_indices[pos + 1]
            training_pairs.append((padded_context, target_index))
    return training_pairs


# ── weights I/O ────────────────────────────────────────────────────────────────

def save_weights(model: TrainableModel, token_to_index: dict, index_to_token: dict, path: str):
    payload = {
        'hyper': {
            'vocab_size':     model.vocab_size,
            'embed_dim':      model.embed_dim,
            'hidden_size':    model.hidden_size,
            'context_length': _CONTEXT_LENGTH,
        },
        'vocab': {
            'token_to_index': token_to_index,
            'index_to_token': index_to_token,
        },
        'weights': {
            'embedding_matrix': model.embedding_matrix,
            'hidden_weights':   model.hidden_weights,
            'hidden_bias':      model.hidden_bias,
            'output_weights':   model.output_weights,
            'output_bias':      model.output_bias,
        },
    }
    with open(path, 'w') as fh:
        json.dump(payload, fh)
    print(f"Weights saved → {path}")


def load_for_training(path: str):
    """Load weights.json into a TrainableModel. Returns (model, token_to_index, index_to_token)."""
    with open(path) as fh:
        payload = json.load(fh)

    hyper = payload['hyper']
    token_to_index = payload['vocab']['token_to_index']
    index_to_token = {int(k): v for k, v in payload['vocab']['index_to_token'].items()}

    model = TrainableModel.__new__(TrainableModel)
    model.vocab_size  = hyper['vocab_size']
    model.embed_dim   = hyper['embed_dim']
    model.hidden_size = hyper['hidden_size']
    model.input_dim   = hyper['context_length'] * hyper['embed_dim']

    w = payload['weights']
    model.embedding_matrix = w['embedding_matrix']
    model.hidden_weights   = w['hidden_weights']
    model.hidden_bias      = w['hidden_bias']
    model.output_weights   = w['output_weights']
    model.output_bias      = w['output_bias']

    return model, token_to_index, index_to_token


# ── training loop ──────────────────────────────────────────────────────────────

def train_loop(model: TrainableModel, training_data: list, epochs: int, lr: float):
    for epoch in range(1, epochs + 1):
        random.shuffle(training_data)
        total_loss = 0.0
        for context_window, target_idx in training_data:
            input_emb, hidden_act, output_probs = model.forward(context_window)
            total_loss -= math.log(max(output_probs[target_idx], 1e-9))
            model.step(context_window, input_emb, hidden_act, output_probs, target_idx, lr)
        avg_loss = total_loss / len(training_data)
        ppl = math.exp(min(avg_loss, 20))
        print(f"  epoch {epoch:>4}/{epochs}  loss={avg_loss:.4f}  ppl={ppl:.2f}")


# ── entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Train or fine-tune the SQL next-word prediction model')
    subparsers = parser.add_subparsers(dest='command', required=True)

    train_parser = subparsers.add_parser('train', help='Train or fine-tune the model')
    train_parser.add_argument('--corpus',   required=True, metavar='FILE',
                              help='Training corpus — one SQL statement per line')
    train_parser.add_argument('--finetune', action='store_true',
                              help='Load existing weights and continue training')
    train_parser.add_argument('--weights',  metavar='FILE', default=_WEIGHTS_PATH,
                              help=f'Weights file to load for fine-tuning (default: {_WEIGHTS_PATH})')
    train_parser.add_argument('--output',   metavar='FILE', default=_WEIGHTS_PATH,
                              help=f'Where to save trained weights (default: {_WEIGHTS_PATH})')
    train_parser.add_argument('--epochs',   type=int,   default=EPOCHS,
                              help=f'Training epochs (default: {EPOCHS})')
    train_parser.add_argument('--lr',       type=float, default=LEARNING_RATE,
                              help=f'Learning rate (default: {LEARNING_RATE})')
    train_parser.add_argument('--debug',    action='store_true',
                              help='Print tokenization output for each training sentence')

    infer_parser = subparsers.add_parser('infer', help='Run inference on a SQL prefix')
    infer_parser.add_argument('--sql',     required=True, metavar='TEXT',
                              help='SQL prefix to complete')
    infer_parser.add_argument('--weights', metavar='FILE', default=_WEIGHTS_PATH,
                              help=f'Weights file to load (default: {_WEIGHTS_PATH})')
    infer_parser.add_argument('--top-k',  type=int, default=10, dest='top_k',
                              help='Number of predictions to show (default: 10)')

    args = parser.parse_args()

    if args.command == 'infer':
        model, t2i, i2t = _load_weights(args.weights)
        results = _predict_next(args.sql, model, t2i, i2t, _VOCAB_VALUES, top_k=args.top_k)
        for token, prob in results:
            print(f"{prob:.4f}  {token}")

    else:
        random.seed(42)

        if args.finetune:
            model, t2i, i2t = load_for_training(args.weights)
            print(f"Fine-tuning from {args.weights}  (vocab_size={model.vocab_size})")
        else:
            t2i = {token: idx for idx, token in _VOCABULARY.items()}
            i2t = {idx: token for idx, token in _VOCABULARY.items()}
            vocab_size = max(t2i.values()) + 1
            model = TrainableModel(vocab_size)
            print(f"Training from scratch  (vocab_size={vocab_size})")

        corpus = load_corpus(args.corpus)
        print(f"Corpus: {len(corpus)} statements")

        vocab_values = set(t2i.keys())
        training_data = make_dataset(corpus, t2i, vocab_values, debug=args.debug)
        print(f"Samples: {len(training_data)}  epochs: {args.epochs}  lr: {args.lr}")

        train_loop(model, training_data, args.epochs, args.lr)
        save_weights(model, t2i, i2t, args.output)
