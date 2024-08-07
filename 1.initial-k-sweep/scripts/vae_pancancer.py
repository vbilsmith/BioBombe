"""
Gregory Way 2017
Variational Autoencoder - Pan Cancer
scripts/vae_pancancer.py

Usage:

    Run in command line with required command arguments:

        python scripts/vae_pancancer.py --learning_rate
                                        --batch_size
                                        --epochs
                                        --kappa
                                        --depth
                                        --output_filename
                                        --num_components
                                        --scale
                                        --subset_mad_genes
                                        --dataset

    Typically, arguments to this script are compiled automatically.

    See `scripts/num_components_paramsweep.py` for more details

Output:

    Loss and validation loss for the specific model trained
"""

import os
import argparse
import numpy as np
import pandas as pd

from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from keras.layers import Input, Dense, Lambda, Layer, Activation
from keras.layers import BatchNormalization
from keras.models import Model, Sequential
from keras import backend as K
from keras import metrics, optimizers
from keras.callbacks import Callback

#defines median absolute deviation function
def mad(df):
    row_medians = df.median(axis='columns', numberic_only=True)
    abs_row_median_diffs = abs(df.sub(row_medians, axis='rows'))
    return abs_row_median_diffs.median(axis='columns', numeric_only=True)

parser = argparse.ArgumentParser()
parser.add_argument('-l', '--learning_rate',
                    help='learning rate of the optimizer', default=0.0005)
parser.add_argument('-b', '--batch_size',
                    help='Number of samples to include in each learning batch', default=50)
parser.add_argument('-e', '--epochs',
                    help='How many times to cycle through the full dataset', default=50)
parser.add_argument('-k', '--kappa',
                    help='How fast to linearly ramp up KL loss', default=0)
parser.add_argument('-d', '--depth', default=1,
                    help='Number of layers between input and latent layer')
parser.add_argument('-c', '--first_layer',
                    help='Dimensionality of the first hidden layer',
                    default=100)
parser.add_argument('-f', '--output_filename',
                    help='The name of the file to store results')
parser.add_argument('-n', '--num_components', default=100,
                    help='The latent space dimensionality to test')
parser.add_argument('-s', '--scale', action='store_true',
                    help='Add decision to scale input data')
parser.add_argument('-m', '--subset_mad_genes', default=8000,
                    help='The number of mad genes to subset')
parser.add_argument('-x', '--dataset', default='GREIN_human',
                    help='the dataset to use in the sweep')
args = parser.parse_args()

# Set hyper parameters
print(args)
learning_rate = float(args.learning_rate)
batch_size = int(args.batch_size)
epochs = int(args.epochs)
kappa = float(args.kappa)
depth = int(args.depth)
first_layer = int(args.first_layer)
output_filename = args.output_filename
latent_dim = int(args.num_components)
scale = args.scale
subset_mad_genes = args.subset_mad_genes
dataset = args.dataset

# Load Data
file = 'train_{}_expression_matrix_processed.tsv.gz'.format(dataset.lower())
rnaseq_file = os.path.join('..', '0.expression-download', 'data', file)
rnaseq_df = pd.read_table(rnaseq_file, index_col=0)

# Determine most variably expressed genes and subset 
if subset_mad_genes is not None:  #if there are any genes in the MAD 
    #mad_genes = abs(rnaseq_df.drop(['gene', 'gene_symbol'], axis = 1) - rnaseq_df.drop(['gene', 'gene_symbol'], axis = 1).median()).median().reset_index()
    mad_genes = mad(rnaseq_df).reset_index()
    top_mad_genes = mad_genes.iloc[0:subset_mad_genes, ].index
    rnaseq_df = rnaseq_df.loc[:, top_mad_genes]

# Zero One normalize input data
if scale:
    scaler = MinMaxScaler()
    x = scaler.fit_transform(rnaseq_df)
    rnaseq_df = pd.DataFrame(x, index=rnaseq_df.index,
                             columns=rnaseq_df.columns)

# Set architecture dimensions
original_dim = rnaseq_df.shape[1]
epsilon_std = 1.0
beta = K.variable(0)
if depth == 2:
    latent_dim2 = int(first_layer)

# Random seed
seed = int(np.random.randint(low=0, high=10000, size=1))
np.random.seed(seed)


# Function for reparameterization trick to make model differentiable
def sampling(args):

    # Function with args required for Keras Lambda function
    z_mean, z_log_var = args

    # Draw epsilon of the same shape from a standard normal distribution
    epsilon = K.random_normal(shape=tf.shape(z_mean), mean=0.,
                              stddev=epsilon_std)

    # The latent vector is non-deterministic and differentiable
    # in respect to z_mean and z_log_var
    z = z_mean + K.exp(z_log_var / 2) * epsilon
    return z


class CustomVariationalLayer(Layer):
    """
    Define a custom layer that learns and performs the training

    """
    def __init__(self, **kwargs):
        # https://keras.io/layers/writing-your-own-keras-layers/
        self.is_placeholder = True
        super(CustomVariationalLayer, self).__init__(**kwargs)

    def vae_loss(self, x_input, x_decoded):
        reconstruction_loss = original_dim * \
                              metrics.binary_crossentropy(x_input, x_decoded)
        kl_loss = - 0.5 * K.sum(1 + z_log_var_encoded -
                                K.square(z_mean_encoded) -
                                K.exp(z_log_var_encoded), axis=-1)
        return K.mean(reconstruction_loss + (K.get_value(beta) * kl_loss))

    def call(self, inputs):
        x = inputs[0]
        x_decoded = inputs[1]
        loss = self.vae_loss(x, x_decoded)
        self.add_loss(loss, inputs=inputs)
        # We won't actually use the output.
        return x


class WarmUpCallback(Callback):
    def __init__(self, beta, kappa):
        self.beta = beta
        self.kappa = kappa

    # Behavior on each epoch
    def on_epoch_end(self, epoch, logs={}):
        if K.get_value(self.beta) <= 1:
            K.set_value(self.beta, K.get_value(self.beta) + self.kappa)

# Process data

# Split 10% test set randomly
test_set_percent = 0.1
rnaseq_test_df = rnaseq_df.sample(frac=test_set_percent)
rnaseq_train_df = rnaseq_df.drop(rnaseq_test_df.index)

# Input place holder for RNAseq data with specific input size
rnaseq_input = Input(shape=(original_dim, ))

# ~~~~~~~~~~~~~~~~~~~~~~
# ENCODER
# ~~~~~~~~~~~~~~~~~~~~~~
# Depending on the depth of the model, the input is eventually compressed into
# a mean and log variance vector of prespecified size. Each layer is
# initialized with glorot uniform weights and each step (dense connections,
# batch norm,and relu activation) are funneled separately
#
# Each vector of length `latent_dim` are connected to the rnaseq input tensor
# In the case of a depth 2 architecture, input_dim -> latent_dim -> latent_dim2

if depth == 1:
    z_shape = latent_dim
    z_mean_dense = Dense(latent_dim,
                         kernel_initializer='glorot_uniform')(rnaseq_input)
    z_log_var_dense = Dense(latent_dim,
                            kernel_initializer='glorot_uniform')(rnaseq_input)
elif depth == 2:
    z_shape = latent_dim2
    hidden_dense = Dense(latent_dim,
                         kernel_initializer='glorot_uniform')(rnaseq_input)
    hidden_dense_batchnorm = BatchNormalization()(hidden_dense)
    hidden_enc = Activation('relu')(hidden_dense_batchnorm)

    z_mean_dense = Dense(latent_dim2,
                         kernel_initializer='glorot_uniform')(hidden_enc)
    z_log_var_dense = Dense(latent_dim2,
                            kernel_initializer='glorot_uniform')(hidden_enc)

z_mean_dense_batchnorm = BatchNormalization()(z_mean_dense)
z_mean_encoded = Activation('relu')(z_mean_dense_batchnorm)

z_log_var_dense_batchnorm = BatchNormalization()(z_log_var_dense)
z_log_var_encoded = Activation('relu')(z_log_var_dense_batchnorm)

# return the encoded and randomly sampled z vector
# Takes two keras layers as input to the custom sampling function layer with a
# latent_dim` output
z = Lambda(sampling,
           output_shape=(z_shape, ))([z_mean_encoded, z_log_var_encoded])

# ~~~~~~~~~~~~~~~~~~~~~~
# DECODER
# ~~~~~~~~~~~~~~~~~~~~~~
# The layers are different depending on the prespecified depth.
#
# Single layer: glorot uniform initialized and sigmoid activation.
# Double layer: relu activated hidden layer followed by sigmoid reconstruction
if depth == 1:
    decoder_to_reconstruct = Dense(original_dim,
                                   kernel_initializer='glorot_uniform',
                                   activation='sigmoid')
elif depth == 2:
    decoder_to_reconstruct = Sequential()
    decoder_to_reconstruct.add(Dense(latent_dim,
                                     kernel_initializer='glorot_uniform',
                                     activation='relu',
                                     input_dim=latent_dim2))
    decoder_to_reconstruct.add(Dense(original_dim,
                                     kernel_initializer='glorot_uniform',
                                     activation='sigmoid'))

rnaseq_reconstruct = decoder_to_reconstruct(z)

# ~~~~~~~~~~~~~~~~~~~~~~
# CONNECTIONS
# ~~~~~~~~~~~~~~~~~~~~~~
adam = optimizers.Adam(lr=learning_rate)
vae_layer = CustomVariationalLayer()([rnaseq_input, rnaseq_reconstruct])
vae = Model(rnaseq_input, vae_layer)
vae.compile(optimizer=adam, loss=None, loss_weights=[beta])

# fit Model
hist = vae.fit(np.array(rnaseq_train_df),
               shuffle=True,
               epochs=epochs,
               batch_size=batch_size,
               validation_data=(np.array(rnaseq_test_df), None),
               callbacks=[WarmUpCallback(beta, kappa)])

# Save training performance
history_df = pd.DataFrame(hist.history)
history_df = history_df.assign(num_components=latent_dim,
                               learning_rate=learning_rate,
                               batch_size=batch_size,
                               epochs=epochs,
                               kappa=kappa,
                               seed=seed,
                               depth=depth,
                               first_layer=first_layer,
                               dataset=dataset)
history_df.to_csv(output_filename, sep='\t')
