import * as THREE from "three";
import { GLTFLoader } from
  "https://cdn.jsdelivr.net/npm/three@0.180.0/examples/jsm/loaders/GLTFLoader.js";

export class AssetLoader {
  constructor() {
    this.loader = new GLTFLoader();
    this.cache = new Map();
  }

  async load(url, options = {}) {
    const {
      scale = 1,
      position = [0, 0, 0],
      rotation = [0, 0, 0],
      castShadow = true,
      receiveShadow = true,
      cache = true
    } = options;

    let gltf;

    if (cache && this.cache.has(url)) {
      gltf = this.cache.get(url);
    } else {
      gltf = await this.loader.loadAsync(url);

      if (cache) {
        this.cache.set(url, gltf);
      }
    }

    const model = gltf.scene.clone(true);

    model.position.set(...position);
    model.rotation.set(...rotation);
    model.scale.setScalar(scale);

    model.traverse((object) => {
      if (!object.isMesh) return;

      object.castShadow = castShadow;
      object.receiveShadow = receiveShadow;

      if (object.material) {
        object.material = Array.isArray(object.material)
          ? object.material.map(material => material.clone())
          : object.material.clone();
      }
    });

    const mixer = gltf.animations.length
      ? new THREE.AnimationMixer(model)
      : null;

    const clips = gltf.animations;
    const actions = new Map();

    if (mixer) {
      for (const clip of clips) {
        actions.set(clip.name, mixer.clipAction(clip));
      }
    }

    return {
      model,
      mixer,
      clips,
      actions,
      play(name, options = {}) {
        if (!mixer) return null;

        const action = actions.get(name);
        if (!action) {
          throw new Error(`Animation not found: ${name}`);
        }

        action.reset();
        action.setLoop(
          options.loop === false ? THREE.LoopOnce : THREE.LoopRepeat,
          options.loop === false ? 1 : Infinity
        );

        if (options.loop === false) {
          action.clampWhenFinished = true;
        }

        action.play();
        return action;
      },
      update(deltaSeconds) {
        mixer?.update(deltaSeconds);
      },
      dispose() {
        mixer?.stopAllAction();
        mixer?.uncacheRoot(model);
      }
    };
  }

  clearCache() {
    this.cache.clear();
  }
}
