install-conda:
	mkdir -p conda
	curl -L -o conda/miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
	chmod +x conda/miniconda.sh
	conda/miniconda.sh -b -p conda
	conda/bin/conda update conda
	conda/bin/conda init

create-env:
	conda create -y -n my_env -c conda-forge python=3.7 pip

install-redun:
	conda run -n my_env pip3 install -e ../../..

teardown:
	conda env remove -n my_env
	rm -rf conda
