

use workload::*;
use rand::distributions::{
    Distribution,
    Uniform,
};

use rand::{
    thread_rng,
    Rng,
};

use zipf::ZipfDistribution;


#[derive(Clone)]
pub enum YCSBOps {
    Read(usize),
    Update(usize, YCSBEntry),
}


#[derive(Copy, Clone)]
pub enum YCSBMode{
    ReadFirst,
    WriteFirst, 
    Random, 
    Interleave,
}


#[derive(Copy, Clone)]
pub enum YCSBSampler {
    Uniform(usize),
    Zipf(usize, f64),
}



#[derive(Copy, Clone)]
pub struct YCSBConfig {
    //load_mixer: LoadMixer,
    pub rw_ratio_: f64,
    pub rw_mode_: YCSBMode,
    pub sampler_name_ : YCSBSampler, 
    pub num_ops_ : usize,
}



pub struct Generator {
    key_sampler_: YCSBSampler,
}


impl Generator {
    pub fn new(config: &YCSBConfig) -> Generator {
        Generator {
            key_sampler_: config.sampler_name_,
        }
    }


    fn make_keys(&self, num: usize) -> Vec<usize> {
        let mut rng = thread_rng();
        match self.key_sampler_ {
            YCSBSampler::Uniform(high) => Uniform::new(0, high).sample_iter(&mut rng).take(num).collect(),
            YCSBSampler::Zipf(high, exp) => ZipfDistribution::new(high,exp).unwrap().sample_iter(&mut rng).take(num).collect(),
        }
    }

    fn make_values(&self, num: usize) -> Vec<YCSBEntry> {
        let mut rng = thread_rng();
        let mut values = Vec::with_capacity(num); 

        for _i in 0..num {
            let entry = YCSBEntry::default();
            values.push(entry);
        }

        return values;
    }


    pub fn make_ops(&self, config: &YCSBConfig) -> Vec<YCSBOps> {
        let num_ops = config.num_ops_;
        let ratio = config.rw_ratio_;
         
        let read_keys = self.make_keys((ratio * num_ops as f64 ) as usize);
        let write_keys = self.make_keys(((1.0- ratio) * num_ops as f64) as usize); 
        let write_values = self.make_values(((1.0-ratio) * num_ops as f64) as usize);

        match config.rw_mode_ {
            YCSBMode::Random => {
                let w_iter = write_keys.into_iter()
                    .zip(write_values.into_iter())
                    .map(|(k, v)| YCSBOps::Update(k, v));

                let r_iter = read_keys.into_iter()
                    .map(|k| YCSBOps::Read(k));

                let mut ops: Vec<YCSBOps> =  w_iter.chain(r_iter).collect();
                thread_rng().shuffle(ops.as_mut_slice());

                return ops
            },
            _ => panic!("not implemented oops")
        }
    }
}
