

use workload::*;
use rand::distributions::{
    Distribution,
    Uniform,
};

use rand::{
    thread_rng,
};

use zipf::ZipfDistribution;


enum YCSBOps {
    Read(usize),
    Update(usize, YCSBEntry),
}


#[derive(Copy, Clone)]
pub enum YCSBSampler {
    Uniform(usize),
    Zipf(usize, f64),
}


pub struct YCSBConfig {
    //load_mixer: LoadMixer,
    pub max_keys_: usize,
    pub num_keys_: usize,
    pub sampler_name_ : YCSBSampler, 
}


pub struct Generator {
    max_keys_: usize,
    num_keys_ : usize,
    key_sampler_: YCSBSampler,
}


impl Generator {
    pub fn new(config: &YCSBConfig) -> Generator {
        Generator {
            max_keys_ : config.max_keys_,
            num_keys_: config.num_keys_,
            key_sampler_: config.sampler_name_,
        }
    }


    pub fn make_keys(&self, num: usize) -> Vec<usize> {
        let mut rng = thread_rng();
        let num = std::cmp::min(self.num_keys_, num);
        match self.key_sampler_ {
            YCSBSampler::Uniform(high) => Uniform::new(0, high).sample_iter(&mut rng).take(num).collect(),
            YCSBSampler::Zipf(num, exp) => ZipfDistribution::new(num,exp).unwrap().sample_iter(&mut rng).take(num).collect(),
        }
    }
}
