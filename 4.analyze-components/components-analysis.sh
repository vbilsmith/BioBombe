#!/bin/bash

# The following scripts will reproduce the figures and results for the
# compression component analysis module

jupyter nbconvert --to=script --FilesWriter.build_directory=scripts/nbconverted *.ipynb
echo "Step1 done!"

Rscript --vanilla scripts/nbconverted/1.visualize-reconstruction.r
echo "Step2 done!"
Rscript --vanilla scripts/nbconverted/2.visualize-sample-correlation.r
echo "Step3 done"
Rscript --vanilla scripts/nbconverted/3.sample-correlation-main-figures.r
echo "Step4 done!"
